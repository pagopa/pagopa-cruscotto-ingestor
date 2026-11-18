package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Reads one {@code .csv.gz} FDR file and hands the mapped rows over in batches.
 *
 * <p>The middle of the pipeline: it takes the compressed stream the storage side opened, and gives
 * back the counts the workflow needs. It does not open the blob, does not write anything, and does
 * not decide the fate of the day.
 *
 * <p><strong>Nothing is held whole.</strong> The stream is decompressed, decoded and split line by
 * line, and the rows leave in batches of {@code fdr.processing.batch-size}. A 400 MB file therefore
 * costs a few buffers and one batch, whatever its size — provided the handler does not keep what it
 * is given, which is the one way to reintroduce the problem.
 *
 * <p><strong>A bad row is not a bad file.</strong> Rows that cannot be split or mapped are counted,
 * classified and left behind, and the read continues: the customer's rule is that an anomalous FDR
 * must be ingested and seen, not rejected. Only the failures that make the rest meaningless —
 * corrupt archive, invalid encoding, a header that does not match — stop the file, as
 * {@link FdrFileFormatException}.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrFileReader {

    /**
     * How many rejected rows are kept for diagnosis.
     *
     * <p>The counts are complete; this is only the sample that travels with them. Without a cap, a
     * file whose every row fails — a dataset exported with a changed timestamp format would do it —
     * would build one object per row, and the reader that exists to bound memory would be the one
     * exhausting it.
     */
    static final int MAX_SAMPLED_REJECTIONS = 20;

    /**
     * Decompression buffer. The default is 512 bytes, which on a 400 MB file means a system call
     * for every half kilobyte; 64 KB is the usual compromise and costs one allocation per file.
     */
    private static final int GZIP_BUFFER_BYTES = 64 * 1024;

    private static final int READER_BUFFER_CHARS = 64 * 1024;

    private final int batchSize;

    public FdrFileReader(FdrProperties fdrProperties) {
        this.batchSize = fdrProperties.getProcessing().getBatchSize();
    }

    /**
     * Reads one file from its compressed stream.
     *
     * @param compressedContent the raw blob stream, still GZIP. Closed by this method, together
     *                          with everything wrapped around it
     * @param file              the file being read; supplies the dataset and the business day, and
     *                          the day is the folder's, never the clock's
     * @param mapper            the mapper for the dataset, see {@link FdrRowMapper#forDataset}
     * @param handler           receives the batches
     * @return the counts of the read, never {@code null}
     * @throws FdrFileFormatException when the file cannot be read as a whole
     */
    public <T extends FdrBusinessRow> FdrFileReadResult read(InputStream compressedContent,
                                                             FdrBlobFile file,
                                                             FdrRowMapper<T> mapper,
                                                             FdrBatchHandler<T> handler) {
        Objects.requireNonNull(compressedContent, "compressedContent");
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(mapper, "mapper");
        Objects.requireNonNull(handler, "handler");

        long startedAt = System.currentTimeMillis();
        ReadState state = new ReadState();

        // Two resources, and the order matters: the raw stream is acquired first, so that it is
        // closed even when open() fails building the chain on top of it. A single resource would
        // leak the blob's HTTP connection on exactly the files that fail — which are the ones a day
        // sees many of when something upstream goes wrong.
        try (InputStream raw = compressedContent;
             BufferedReader reader = open(raw)) {
            FdrCsvHeader header = readHeader(reader, file);
            readRows(reader, header, file, mapper, handler, state);
        } catch (ZipException | EOFException e) {
            // EOFException is what a GZIP stream cut short raises: a download interrupted mid-file
            // looks exactly like this, and it is the archetypal retryable failure.
            throw new FdrFileFormatException(FdrFileFormatException.Kind.CORRUPT_ARCHIVE,
                    "FDR file " + file.blobName() + " is not a readable GZIP archive", e);
        } catch (CharacterCodingException e) {
            throw new FdrFileFormatException(FdrFileFormatException.Kind.INVALID_ENCODING,
                    "FDR file " + file.blobName() + " is not valid UTF-8", e);
        } catch (IOException e) {
            throw new FdrFileFormatException(FdrFileFormatException.Kind.READ_FAILED,
                    "Unable to read FDR file " + file.blobName(), e);
        }

        FdrFileReadResult result = state.toResult(file, System.currentTimeMillis() - startedAt);
        logRead(result);
        return result;
    }

    /**
     * Wraps the raw stream into the decoding chain.
     *
     * <p>The decoder is built by hand instead of using {@code new InputStreamReader(in, UTF_8)}
     * because the convenient form <em>replaces</em> malformed input with U+FFFD and says nothing.
     * On a damaged download that produces a file of rows that parse, map and insert, with
     * identifiers quietly altered. Reporting turns the same event into one failure, once, naming
     * the file.
     */
    private static BufferedReader open(InputStream compressedContent) throws IOException {
        GZIPInputStream gzip = new GZIPInputStream(compressedContent, GZIP_BUFFER_BYTES);
        InputStreamReader decoded = new InputStreamReader(gzip, StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT));
        return new BufferedReader(decoded, READER_BUFFER_CHARS);
    }

    private static FdrCsvHeader readHeader(BufferedReader reader, FdrBlobFile file)
            throws IOException {
        String headerLine = reader.readLine();
        if (headerLine == null) {
            throw new FdrFileFormatException(FdrFileFormatException.Kind.MISSING_HEADER,
                    "FDR file " + file.blobName() + " is empty: no header line");
        }
        List<String> cells;
        try {
            cells = FdrCsvParser.parseLine(FdrCsvParser.stripBom(headerLine));
        } catch (FdrMalformedLineException e) {
            throw new FdrFileFormatException(FdrFileFormatException.Kind.UNEXPECTED_HEADER,
                    "FDR file " + file.blobName() + " has an unreadable header line", e);
        }
        return FdrCsvHeader.of(cells, file.dataset());
    }

    private <T extends FdrBusinessRow> void readRows(BufferedReader reader, FdrCsvHeader header,
                                                     FdrBlobFile file, FdrRowMapper<T> mapper,
                                                     FdrBatchHandler<T> handler, ReadState state)
            throws IOException {
        LocalDate day = file.businessDay();
        List<T> batch = new ArrayList<>(batchSize);
        String line;

        // Line 1 is the header, so the first data line is 2 and the numbers match a text editor.
        long lineNumber = 1;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            if (line.isEmpty()) {
                // Only a truly empty line is skipped. A line of separators is a row of empty
                // values, which is data, and deciding otherwise here would delete it silently.
                state.blankLines++;
                continue;
            }
            state.dataRows++;

            T row = mapRow(line, lineNumber, header, mapper, day, state);
            if (row == null) {
                continue;
            }
            batch.add(row);
            if (batch.size() >= batchSize) {
                handler.accept(batch);
                state.mappedRows += batch.size();
                batch = new ArrayList<>(batchSize);
            }
        }

        if (!batch.isEmpty()) {
            handler.accept(batch);
            state.mappedRows += batch.size();
        }
    }

    /** @return the mapped row, or {@code null} when the row was rejected and recorded */
    private static <T extends FdrBusinessRow> T mapRow(String line, long lineNumber,
                                                       FdrCsvHeader header, FdrRowMapper<T> mapper,
                                                       LocalDate day, ReadState state) {
        List<String> fields;
        try {
            fields = FdrCsvParser.parseLine(line);
        } catch (FdrMalformedLineException e) {
            state.reject(FdrRowRejection.malformedLine(lineNumber, e.getMessage()));
            return null;
        }
        try {
            return mapper.map(header, fields, day);
        } catch (FdrValueException e) {
            state.reject(FdrRowRejection.fromValue(lineNumber, e, header.columnCount(),
                    fields.size()));
            return null;
        }
    }

    /**
     * Mutable counters of one read.
     *
     * <p>A small mutable holder rather than a handful of local variables because the loop is split
     * across methods, and rather than fields on the service because the service is a singleton and
     * two days could be read at once.
     */
    private static final class ReadState {

        private long dataRows;
        private long mappedRows;
        private long blankLines;
        private long rejectedRows;
        private final Map<FdrRowRejection.Reason, Long> byReason =
                new EnumMap<>(FdrRowRejection.Reason.class);
        private final List<FdrRowRejection> sample = new ArrayList<>();

        void reject(FdrRowRejection rejection) {
            rejectedRows++;
            byReason.merge(rejection.reason(), 1L, Long::sum);
            if (sample.size() < MAX_SAMPLED_REJECTIONS) {
                sample.add(rejection);
            }
        }

        FdrFileReadResult toResult(FdrBlobFile file, long elapsedMs) {
            return new FdrFileReadResult(file.dataset(), file.blobName(), file.businessDay(),
                    dataRows, mappedRows, rejectedRows, blankLines,
                    Map.copyOf(byReason), List.copyOf(sample), elapsedMs);
        }
    }

    /**
     * One synthetic line per file, as the logging guidance requires for an I/O boundary.
     *
     * <p>Never one per row, and never one per batch either: a day holds millions of rows and tens
     * of batches, and what gets read is the file's summary. X and N are both on it because the
     * outcome rules are stated in terms of them — a reader looking for "why is this day
     * COMPLETED_WITH_WARNING" finds the answer without opening anything else.
     */
    private static void logRead(FdrFileReadResult result) {
        try (MdcScope ignored = MdcScope.phase("FDR_FILE_READ");
             LogDetails details = LogDetails.open()
                     .with(LogFields.DEPENDENCY, "azure-blob")
                     .with("dataset", result.dataset().name())
                     .with("blob", result.blobName())
                     .with("business_day", result.businessDay().toString())
                     .with("data_rows", result.dataRows())
                     .with("mapped_rows", result.mappedRows())
                     .with("rejected_rows", result.rejectedRows())
                     .with("blank_lines", result.blankLines())
                     .with(LogFields.ELAPSED_MS, result.elapsedMs())) {
            log.info("Read FDR file");
        }

        if (result.rejectedRows() == 0) {
            return;
        }
        logRejections(result);
    }

    /**
     * Rejections summarised by reason, with a bounded sample at DEBUG.
     *
     * <p>The counts go one key per reason and as <strong>numbers</strong>: a map rendered into a
     * text field reads but does not aggregate, and the question actually asked — "how many
     * timestamps did we fail to parse this week?" — would stay unanswered. The keys are bounded by
     * the enum, so the line cannot grow unexpectedly.
     */
    private static void logRejections(FdrFileReadResult result) {
        try (MdcScope ignored = MdcScope.phase("FDR_FILE_READ");
             LogDetails details = LogDetails.open()
                     .with("dataset", result.dataset().name())
                     .with("blob", result.blobName())
                     .with("business_day", result.businessDay().toString())
                     .with("data_rows", result.dataRows())
                     .with("rejected_rows", result.rejectedRows())) {
            result.rejectionsByReason().forEach((reason, count) ->
                    details.with("rejected_" + reason.name().toLowerCase(Locale.ROOT), count));
            log.warn("Skipped FDR rows that could not be mapped");
        }

        result.rejectionSample().forEach(rejection ->
                log.debug("Skipped FDR row at line {} ({} on column {}): {}",
                        rejection.lineNumber(), rejection.reason(), rejection.column(),
                        rejection.detail()));
    }
}
