package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplateDetector.TemplateDetection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Shared, streaming reader of a Massive Search perimeter CSV held in memory as a String (the
 * generated perimeter for FILTER instances, the uploaded file for CSV instances).
 *
 * <p>Reads the header once to detect the {@link CsvTemplate}, then streams the data rows to the
 * supplied {@link BatchHandler} in bounded, <em>de-duplicated</em> batches. Duplicate rows (same
 * {@code nav/pa/iuv/token} tuple as normalized by {@link CsvInputReader#toRow}) are ignored and
 * skipped, per the client specification; blank lines are skipped. Nothing beyond the current batch
 * and the set of seen keys is retained, so memory stays bounded relative to the distinct rows.</p>
 *
 * <p>This centralizes the read/dedup/batch loop formerly duplicated in every report generator.</p>
 */
@Slf4j
@Component
public class PerimeterCsvReader {

    private final CsvInputReader inputReader;
    private final CsvTemplateDetector templateDetector;

    public PerimeterCsvReader(CsvInputReader inputReader, CsvTemplateDetector templateDetector) {
        this.inputReader = inputReader;
        this.templateDetector = templateDetector;
    }

    /**
     * Streams the perimeter rows in de-duplicated batches.
     *
     * @param content          the CSV content; a {@code null} or header-only input yields no batches
     * @param fallbackTemplate template to use when the header alone does not resolve one (may be {@code null})
     * @param batchSize        maximum keys per batch (values &lt; 1 are treated as 1)
     * @param handler          invoked once per batch; must consume the batch synchronously because the
     *                         underlying list is reused across invocations
     * @return the sum of the values returned by the handler across all batches
     */
    public long forEachBatch(String content, CsvTemplate fallbackTemplate, int batchSize, BatchHandler handler) {
        int effectiveBatch = Math.max(1, batchSize);
        long total = 0L;
        try (BufferedReader reader = inputReader.newReader(content)) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                log.warn("phase=PERIMETER_CSV_EMPTY reason=no-header-row");
                return 0L;
            }
            TemplateDetection detection = templateDetector.detect(inputReader.parseLine(headerLine));
            CsvTemplate template = resolveTemplate(detection, fallbackTemplate);

            Set<SearchInputRow> seen = new HashSet<>();
            List<SearchInputRow> batch = new ArrayList<>(effectiveBatch);
            String line;
            while ((line = reader.readLine()) != null) {
                if (inputReader.isBlank(line)) {
                    continue;
                }
                SearchInputRow row = inputReader.toRow(detection, inputReader.parseLine(line));
                if (!seen.add(row)) {
                    // Duplicate perimeter row: ignore it and proceed (client spec).
                    continue;
                }
                batch.add(row);
                if (batch.size() >= effectiveBatch) {
                    total += handler.handle(template, batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                total += handler.handle(template, batch);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read perimeter CSV", e);
        }
        return total;
    }

    private CsvTemplate resolveTemplate(TemplateDetection detection, CsvTemplate fallbackTemplate) {
        if (detection.template() != null && detection.template() != CsvTemplate.UNKNOWN) {
            return detection.template();
        }
        return fallbackTemplate == null ? CsvTemplate.UNKNOWN : fallbackTemplate;
    }

    /** Receives one de-duplicated batch of perimeter keys and returns a per-batch count to accumulate. */
    @FunctionalInterface
    public interface BatchHandler {
        long handle(CsvTemplate template, List<SearchInputRow> batch);
    }
}
