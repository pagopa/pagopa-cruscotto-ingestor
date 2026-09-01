package it.pagopa.cruscotto.ingestion.massivesearch.report.attempt;

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
 * Report generator producing {@code tentativi.csv} (one row per payment attempt / token), built on the
 * shared {@link AbstractPerimeterReportGenerator} pipeline. It only binds the attempt headers and the
 * set-based attempt query ({@link AttemptReportRepository#streamByKeys}); reading, de-duplication and
 * batching of the perimeter are handled by the base class.
 */
@Component
public class AttemptReportGenerator extends AbstractPerimeterReportGenerator<AttemptReportRow> {

    private final AttemptReportRepository repository;

    public AttemptReportGenerator(
        PerimeterCsvReader perimeterReader,
        CsvLineWriter lineWriter,
        MassiveSearchProperties properties,
        AttemptReportRepository repository
    ) {
        super(perimeterReader, lineWriter, properties);
        this.repository = repository;
    }

    @Override
    public ReportType type() {
        return ReportType.TOKEN;
    }

    @Override
    protected List<String> headers() {
        return AttemptReportColumns.HEADERS;
    }

    @Override
    protected long streamByKeys(CsvTemplate template, List<SearchInputRow> keys,
                                AnalysisWindow window, Consumer<AttemptReportRow> consumer) {
        return repository.streamByKeys(template, keys, window, consumer);
    }
}
