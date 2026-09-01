package it.pagopa.cruscotto.ingestion.massivesearch.report;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvLineWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.PerimeterCsvReader;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionContext;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionException;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.SearchReportGenerator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;
import java.util.function.Consumer;

/**
 * Shared skeleton for the three Massive Search report generators. Owns the common pipeline — write the
 * header, stream the (de-duplicated, batched) perimeter keys through {@link PerimeterCsvReader}, run one
 * set-based query per batch and stream the resulting rows to the report writer — leaving each concrete
 * generator to declare only its {@link #type()}, {@link #headers()} and the batch query
 * ({@link #streamByKeys}). This removes the previously triplicated read/dedup/batch loop.
 *
 * @param <R> the concrete report row type produced by this generator
 */
@Slf4j
public abstract class AbstractPerimeterReportGenerator<R extends ReportRow> implements SearchReportGenerator {

    private final PerimeterCsvReader perimeterReader;
    private final CsvLineWriter lineWriter;
    private final int batchSize;

    protected AbstractPerimeterReportGenerator(
        PerimeterCsvReader perimeterReader,
        CsvLineWriter lineWriter,
        MassiveSearchProperties properties
    ) {
        this.perimeterReader = perimeterReader;
        this.lineWriter = lineWriter;
        this.batchSize = Math.max(1, properties.getExecution().getPerimeterBatchSize());
    }

    @Override
    public long writeReport(MassiveSearchExecutionContext context, Writer writer) throws IOException {
        String content = context.getInputCsvContent();
        if (content == null) {
            throw new MassiveSearchExecutionException(
                "Missing input CSV content for " + type() + " report, instanceId=" + context.getInstanceId());
        }

        lineWriter.writeLine(writer, headers());

        AnalysisWindow window = context.getAnalysisWindow();
        long rows = perimeterReader.forEachBatch(content, context.getInputTemplate(), batchSize,
            (template, batch) -> streamByKeys(template, batch, window, row -> writeRowUnchecked(writer, row)));

        log.info("phase=REPORT_GENERATED report={} instanceId={} executionId={} rows={}",
            type(), context.getInstanceId(), context.getExecutionId(), rows);
        return rows;
    }

    private void writeRowUnchecked(Writer writer, R row) {
        try {
            lineWriter.writeLine(writer, row.values());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Ordered header names of this report. */
    protected abstract List<String> headers();

    /**
     * Streams the report rows matching the given batch of keys within the window, invoking the consumer
     * for each produced row and returning the number of rows produced.
     */
    protected abstract long streamByKeys(CsvTemplate template, List<SearchInputRow> keys,
                                         AnalysisWindow window, Consumer<R> consumer);
}
