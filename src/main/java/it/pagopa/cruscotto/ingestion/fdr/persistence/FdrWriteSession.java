package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFlussoRow;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes the rows of one file, a batch at a time, and keeps the counters.
 *
 * <p>Not a bean: one instance per file, created by {@link FdrRowWriter#openSession}. It is not
 * thread-safe and does not need to be — a file is read by one thread, and the counters would be
 * meaningless if two files shared them.
 *
 * <p>Plugs straight into the reader: {@code reader.read(stream, file, mapper, session::accept)}.
 * The method reference is what adapts the reader's precise {@code List<FdrFlussoRow>} to the
 * {@code List<? extends FdrBusinessRow>} accepted here, so neither side has to know the other's
 * generics.
 */
@Slf4j
public class FdrWriteSession {

    /** Bounded sample of refused rows. The count is complete; see {@link FdrWriteResult}. */
    static final int MAX_ERROR_SAMPLES = 20;

    private final FdrWriteContext context;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final FdrInsertStatement statement;
    private final String sql;
    private final int batchSize;
    private final FdrDayFence fence;
    private final long startedAt = System.currentTimeMillis();

    private final FdrTruncationTally truncations = new FdrTruncationTally();
    private final List<FdrPersistenceError> errorSample = new ArrayList<>();
    /**
     * Conteggio completo per categoria, accanto al campione limitato.
     *
     * <p>Il campione dice <em>com'erano fatte</em> venti righe, questo dice <em>quante</em> ce ne
     * sono di ciascun tipo — ed e' il secondo a servire quando le righe rifiutate sono centinaia di
     * migliaia, perche' distingue "il tracciato e' cambiato" da "ci sono venti righe sporche".
     * Cresce con la dimensione dell'enum, non con quella del file: nessun rischio di memoria.
     */
    private final Map<FdrPersistenceError.Category, Long> rejectionsByCategory =
            new EnumMap<>(FdrPersistenceError.Category.class);
    private long rowsReceived;
    private long rowsInserted;
    private long rowsRejected;
    private long batchesAttempted;
    private long batchesSucceeded;
    private long batchesSplit;
    private FdrWriteResult completed;

    FdrWriteSession(FdrWriteContext context, JdbcTemplate jdbcTemplate,
                    TransactionTemplate transactionTemplate, String schema, int batchSize,
                    FdrDayFence fence) {
        this.fence = fence;
        this.context = context;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.statement = FdrInsertStatement.forDataset(context.dataset());
        this.sql = statement.sql(schema);
        this.batchSize = batchSize;
    }

    /**
     * Writes one batch of rows.
     *
     * <p>The batch arrives already sized by the reader; it is re-chunked here to the same
     * configured size, so that a caller handing over a larger list cannot turn one JDBC batch into
     * an unbounded one. There is no second batch-size knob: both sides read
     * {@code fdr.processing.batch-size}.
     *
     * @throws FdrPersistenceException if a row does not belong to this session's dataset
     * @throws RuntimeException        the original database failure, when it is not attributable
     *                                 to a single row. The file must stop there
     */
    public void accept(List<? extends FdrBusinessRow> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        long firstOrdinal = rowsReceived + 1;
        List<FdrBusinessRow> rows = prepare(batch, firstOrdinal);
        rowsReceived += rows.size();

        for (int start = 0; start < rows.size(); start += batchSize) {
            int end = Math.min(start + batchSize, rows.size());
            write(rows.subList(start, end), firstOrdinal + start);
        }
    }

    /**
     * Checks the dataset and applies the one authorised transformation.
     *
     * <p>Shortening {@code CAUSALE} happens here, before any SQL, and produces a new row rather
     * than mutating one: the row the reader produced keeps the source value, so the only thing
     * that differs between what the file said and what the database holds is this single field,
     * recorded in the tally.
     */
    private List<FdrBusinessRow> prepare(List<? extends FdrBusinessRow> batch, long firstOrdinal) {
        List<FdrBusinessRow> prepared = new ArrayList<>(batch.size());
        long ordinal = firstOrdinal;
        for (FdrBusinessRow row : batch) {
            Objects.requireNonNull(row, "row");
            if (row.dataset() != context.dataset()) {
                throw new FdrPersistenceException(FdrPersistenceException.Kind.DATASET_MISMATCH,
                        "Row of dataset " + row.dataset() + " handed to a "
                                + context.dataset() + " write session");
            }
            prepared.add(row instanceof FdrFlussoRow flusso ? withPersistedCausale(flusso, ordinal)
                    : row);
            ordinal++;
        }
        return prepared;
    }

    private FdrFlussoRow withPersistedCausale(FdrFlussoRow row, long ordinal) {
        String causale = row.causale();
        if (!FdrCausale.isTruncated(causale)) {
            return row;
        }
        truncations.record(FdrCausale.sourceLength(causale), ordinal);
        return new FdrFlussoRow(row.dateInserted(), row.insertedTimestamp(), row.idFlusso(),
                row.dataOraFlusso(), row.dataRegolamento(), FdrCausale.toPersisted(causale),
                row.numPagamenti(), row.sommaVersata(), row.idDominio(), row.psp(), row.intPsp(),
                row.uniqueId(), row.allDates());
    }

    /**
     * One JDBC batch, in its own transaction; on an isolable failure, two halves.
     *
     * <p>The happy path is a single {@code batchUpdate}, which is the only shape that sustains
     * millions of rows a day. The fallback exists for the row the database refuses, and it is
     * bounded by construction: halving terminates at a single row, so a batch with one bad row
     * costs about {@code 2·log2(n)} attempts instead of {@code n} single inserts. The worst case —
     * every row invalid — is {@code 2n-1} attempts, which is the price of still writing nothing
     * wrong, and it is visible in {@code batchesSplit}.
     *
     * <p>A failure that is <em>not</em> attributable to a row is rethrown immediately. Splitting a
     * batch because the connection is closed would produce thousands of attempts against a
     * database already in trouble, and bury the cause under them.
     */
    private void write(List<FdrBusinessRow> rows, long firstOrdinal) {
        batchesAttempted++;
        try {
            transactionTemplate.executeWithoutResult(status -> {
                // PRIMA ISTRUZIONE DELLA TRANSAZIONE, e deve restare tale. L'UPDATE condizionato al
                // token prende i lock delle due righe di stato e li tiene fino al commit: finche'
                // questa transazione e' aperta nessuno puo' subentrare, e se qualcuno e' gia'
                // subentrato l'UPDATE tocca zero righe e si solleva prima di scrivere una riga.
                // Spostarla fuori dalla lambda la farebbe girare in un'altra transazione, i lock
                // verrebbero rilasciati subito e la garanzia svanirebbe senza che nulla lo segnali.
                fence.requireOwnership();
                jdbcTemplate.batchUpdate(sql,
                    new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            statement.bind(ps, rows.get(i));
                        }

                        @Override
                        public int getBatchSize() {
                            return rows.size();
                        }
                    });
            });
            batchesSucceeded++;
            rowsInserted += rows.size();
        } catch (RuntimeException e) {
            if (!FdrSqlErrors.isRowIsolable(e)) {
                throw e;
            }
            if (rows.size() == 1) {
                reject(firstOrdinal, e);
                return;
            }
            // The transaction above has already rolled back — the template commits or rolls back
            // before returning — so the halves start from a clean state and cannot duplicate rows
            // the failed attempt might have written.
            //
            // Ogni meta' apre una transazione NUOVA, quindi rifa' anche la chiusura: non e' uno
            // spreco, e' obbligatorio. I lock di riga presi dal tentativo fallito sono stati
            // rilasciati col suo rollback, quindi senza una nuova verifica le meta' scriverebbero
            // fuori da qualunque garanzia di possesso. Il costo e' limitato dallo stesso tetto del
            // frazionamento: 2n-1 tentativi nel caso peggiore, visibile in batchesSplit.
            batchesSplit++;
            int middle = rows.size() / 2;
            write(rows.subList(0, middle), firstOrdinal);
            write(rows.subList(middle, rows.size()), firstOrdinal + middle);
        }
    }

    private void reject(long rowOrdinal, RuntimeException failure) {
        rowsRejected++;
        FdrPersistenceError error = FdrPersistenceError.of(context, rowOrdinal,
                FdrSqlErrors.sqlStateOf(failure), FdrSqlErrors.messageOf(failure));
        rejectionsByCategory.merge(error.category(), 1L, Long::sum);
        if (errorSample.size() < MAX_ERROR_SAMPLES) {
            errorSample.add(error);
        }
    }

    /**
     * The counters as they stand right now, without closing anything.
     *
     * <p>Exists for the file-level log, which has to describe a file that is still open: with only
     * the final {@link #complete()} an interrupted file would show zeros, and "read nothing" would
     * be indistinguishable from "stopped near the end".
     *
     * <p>Read-only and allocation-light by design — four numbers, no samples — because the caller
     * asks for it once per batch. Copying the error sample or the truncation ordinals here would
     * undo the memory bound this class exists to keep.
     */
    public FdrWriteProgress progress() {
        return new FdrWriteProgress(rowsReceived, rowsInserted, rowsRejected,
                truncations.affectedRows());
    }

    /**
     * Closes the session and returns its counters, logging the summary once.
     *
     * <p>Calling it twice returns the same result without logging again: the orchestration to come
     * may well ask for the counters more than once, and a duplicated summary line would make a
     * file look like two.
     */
    public FdrWriteResult complete() {
        if (completed != null) {
            return completed;
        }
        completed = new FdrWriteResult(context.dataset(), context.businessDay(), context.blobName(),
                rowsReceived, rowsInserted, rowsRejected, batchesAttempted, batchesSucceeded,
                batchesSplit, truncations.toSummary(), Map.copyOf(rejectionsByCategory),
                List.copyOf(errorSample),
                System.currentTimeMillis() - startedAt);
        logWrite(completed);
        return completed;
    }

    /**
     * One synthetic line per file, as the logging guidance requires for an I/O boundary.
     *
     * <p>Never one per row or per batch: a day holds millions of rows, and what gets read is the
     * file's summary. {@code batches_split} is on it because it is the number that says a write
     * cost more than it should have, and {@code rows_rejected} because it is what keeps the day
     * from being {@code COMPLETED}.
     */
    private static void logWrite(FdrWriteResult result) {
        try (MdcScope ignored = MdcScope.phase("FDR_FILE_WRITE");
             LogDetails details = LogDetails.open()
                     .with(LogFields.DEPENDENCY, "postgres")
                     .with(LogFields.TABLE, FdrPartitionGuard.tableName(result.dataset()))
                     .with("dataset", result.dataset().name())
                     .with("blob", result.blobName())
                     .with("business_day", result.businessDay().toString())
                     .with(LogFields.ROWS, result.rowsInserted())
                     .with("rows_received", result.rowsReceived())
                     .with("rows_rejected", result.rowsRejected())
                     .with("batches_attempted", result.batchesAttempted())
                     .with("batches_split", result.batchesSplit())
                     .with("causale_truncated", result.truncation().affectedRows())
                     .with(LogFields.ELAPSED_MS, result.elapsedMs())) {
            log.info("Wrote FDR rows");
        }

        logTruncations(result);
        logRejections(result);
    }

    /**
     * The authorised truncations, aggregated — never one line per row.
     *
     * <p><strong>DEBUG e non WARN</strong>: accorciare {@code CAUSALE} a 30 caratteri e' la
     * trasformazione che il cliente ha chiesto, e su una giornata reale tocca una riga su cinque.
     * Un avvertimento che scatta ogni singolo giorno non e' un avvertimento: e' rumore che nasconde
     * quelli veri. Il cliente ha chiesto che il troncamento sia silenzioso.
     *
     * <p>La riga resta invece di sparire per un numero solo: {@code max_source_len}. 35 e' il
     * profilo noto, e qualcosa sopra significa che la sorgente ha cambiato forma — l'unica cosa
     * qui che valga la pena guardare. Resta raggiungibile alzando il livello, e il conteggio
     * completo e' comunque in {@code FDR_INGESTION_FILE.ROWS_TRUNCATED}.
     */
    private static void logTruncations(FdrWriteResult result) {
        FdrTruncationSummary truncation = result.truncation();
        if (truncation.isEmpty()) {
            return;
        }
        try (MdcScope ignored = MdcScope.phase("FDR_FILE_WRITE");
             LogDetails details = LogDetails.open()
                     .with("dataset", result.dataset().name())
                     .with("blob", result.blobName())
                     .with("business_day", result.businessDay().toString())
                     .with("field", truncation.field())
                     .with("affected_rows", truncation.affectedRows())
                     .with("max_source_len", truncation.maxSourceLength())
                     .with("persisted_len", truncation.persistedLength())) {
            log.debug("Shortened FDR source values to the column width");
        }
    }

    private static void logRejections(FdrWriteResult result) {
        if (result.rowsRejected() == 0) {
            return;
        }
        try (MdcScope ignored = MdcScope.phase("FDR_FILE_WRITE");
             LogDetails details = LogDetails.open()
                     .with("dataset", result.dataset().name())
                     .with("blob", result.blobName())
                     .with("business_day", result.businessDay().toString())
                     .with("rows_rejected", result.rowsRejected())
                     .with("batches_split", result.batchesSplit())) {
            log.warn("Skipped FDR rows the database refused");
        }

        result.errorSample().forEach(error ->
                log.debug("Rejected FDR row {} of {} ({}, SQLSTATE {}): {}", error.rowOrdinal(),
                        error.blobName(), error.category(), error.sqlState(), error.message()));
    }
}
