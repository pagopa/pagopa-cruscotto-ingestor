package it.pagopa.cruscotto.ingestion.massivesearch.report.attempt;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvInputReader;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplateDetector;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplateDetector.TemplateDetection;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionContext;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionException;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.ReportType;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.SearchReportGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link SearchReportGenerator} producing {@code tentativi.csv} (one row per payment attempt / token).
 *
 * <p>Reads the execution input CSV (perimeter for FILTER instances, uploaded file for CSV instances)
 * streaming it in bounded batches of keys and, for every batch, streams the tokens of the matching
 * positions from the SERT tables via {@link AttemptReportRepository} with a single set-based query.
 * Nothing is buffered beyond the current batch of keys.</p>
 */
@Slf4j
@Component
public class AttemptReportGenerator implements SearchReportGenerator {

    private final CsvInputReader csvInputReader;
    private final CsvTemplateDetector templateDetector;
    private final AttemptReportRepository repository;
    private final AttemptCsvExporter exporter;
    private final MassiveSearchProperties properties;

    public AttemptReportGenerator(
        CsvInputReader csvInputReader,
        CsvTemplateDetector templateDetector,
        AttemptReportRepository repository,
        AttemptCsvExporter exporter,
        MassiveSearchProperties properties
    ) {
        this.csvInputReader = csvInputReader;
        this.templateDetector = templateDetector;
        this.repository = repository;
        this.exporter = exporter;
        this.properties = properties;
    }

    @Override
    public ReportType type() {
        return ReportType.TOKEN;
    }

    @Override
    public long writeReport(MassiveSearchExecutionContext context, Writer writer) throws IOException {
        exporter.writeHeader(writer);

        String inputContent = context.getInputCsvContent();
        if (inputContent == null) {
            throw new MassiveSearchExecutionException(
                "Missing input CSV content for attempt report, instanceId=" + context.getInstanceId());
        }

        long rows = 0L;
        try (BufferedReader reader = csvInputReader.newReader(inputContent)) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                log.warn("phase=REPORT_TOKEN_EMPTY instanceId={} executionId={} reason=empty-input",
                    context.getInstanceId(), context.getExecutionId());
                return 0L;
            }
            TemplateDetection detection = templateDetector.detect(csvInputReader.parseLine(headerLine));
            CsvTemplate template = resolveTemplate(detection, context);
            AnalysisWindow window = context.getAnalysisWindow();
            int batchSize = Math.max(1, properties.getExecution().getPerimeterBatchSize());
            List<SearchInputRow> batch = new ArrayList<>(batchSize);

            String line;
            while ((line = reader.readLine()) != null) {
                if (csvInputReader.isBlank(line)) {
                    continue;
                }
                List<String> columns = csvInputReader.parseLine(line);
                batch.add(csvInputReader.toRow(detection, columns));
                if (batch.size() >= batchSize) {
                    rows += streamBatch(template, batch, window, writer);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                rows += streamBatch(template, batch, window, writer);
            }
        }
        return rows;
    }

    private CsvTemplate resolveTemplate(TemplateDetection detection, MassiveSearchExecutionContext context) {
        if (detection.template() != null && detection.template() != CsvTemplate.UNKNOWN) {
            return detection.template();
        }
        return context.getInputTemplate() == null ? CsvTemplate.UNKNOWN : context.getInputTemplate();
    }

    private long streamBatch(CsvTemplate template, List<SearchInputRow> keys, AnalysisWindow window, Writer writer) {
        return repository.streamByKeys(template, keys, window, row -> {
            try {
                exporter.writeRow(writer, row);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
