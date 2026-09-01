package it.pagopa.cruscotto.ingestion.massivesearch.report.transfer;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvLineWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.PerimeterCsvReader;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.ReportType;
import it.pagopa.cruscotto.ingestion.massivesearch.report.AbstractPerimeterReportGenerator;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Consumer;

/**
 * Report generator producing {@code versamenti.csv} (one row per transfer), built on the shared
 * {@link AbstractPerimeterReportGenerator} pipeline. It only binds the transfer headers and the
 * set-based transfer query ({@link TransferReportRepository#streamByKeys}); reading, de-duplication and
 * batching of the perimeter are handled by the base class.
 */
@Component
public class TransferReportGenerator extends AbstractPerimeterReportGenerator<TransferReportRow> {

    private final TransferReportRepository repository;

    public TransferReportGenerator(
        PerimeterCsvReader perimeterReader,
        CsvLineWriter lineWriter,
        MassiveSearchProperties properties,
        TransferReportRepository repository
    ) {
        super(perimeterReader, lineWriter, properties);
        this.repository = repository;
    }

    @Override
    public ReportType type() {
        return ReportType.TRANSFER;
    }

    @Override
    protected List<String> headers() {
        return TransferReportColumns.HEADERS;
    }

    @Override
    protected long streamByKeys(CsvTemplate template, List<SearchInputRow> keys,
                                AnalysisWindow window, Consumer<TransferReportRow> consumer) {
        return repository.streamByKeys(template, keys, window, consumer);
    }
}
