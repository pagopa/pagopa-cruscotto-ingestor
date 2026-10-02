package it.pagopa.cruscotto.ingestion.massivesearch.report.position;

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
 * Report generator producing {@code posizioni.csv} (one row per debit position), built on the shared
 * {@link AbstractPerimeterReportGenerator} pipeline. It only binds the position headers and the
 * set-based position query ({@link PositionReportRepository#streamByKeys}); reading, de-duplication and
 * batching of the perimeter are handled by the base class.
 */
@Component
public class PositionReportGenerator extends AbstractPerimeterReportGenerator<PositionReportRow> {

    private final PositionReportRepository repository;

    public PositionReportGenerator(
        PerimeterCsvReader perimeterReader,
        CsvLineWriter lineWriter,
        MassiveSearchProperties properties,
        PositionReportRepository repository
    ) {
        super(perimeterReader, lineWriter, properties);
        this.repository = repository;
    }

    @Override
    public ReportType type() {
        return ReportType.POSITION;
    }

    @Override
    protected List<String> headers() {
        return PositionReportColumns.HEADERS;
    }

    @Override
    protected long streamByKeys(CsvTemplate template, List<SearchInputRow> keys,
                                AnalysisWindow window, Consumer<PositionReportRow> consumer) {
        return repository.streamByKeys(template, keys, window, consumer);
    }
}
