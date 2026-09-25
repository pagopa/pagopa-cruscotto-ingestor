package it.pagopa.cruscotto.ingestion.massivesearch.report.token;

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
 * Report generator producing {@code tentativi.csv} (one row per payment token), built on the
 * shared {@link AbstractPerimeterReportGenerator} pipeline. It only binds the token headers and the
 * set-based token query ({@link TokenReportRepository#streamByKeys}); reading, de-duplication and
 * batching of the perimeter are handled by the base class.
 */
@Component
public class TokenReportGenerator extends AbstractPerimeterReportGenerator<TokenReportRow> {

    private final TokenReportRepository repository;

    public TokenReportGenerator(
        PerimeterCsvReader perimeterReader,
        CsvLineWriter lineWriter,
        MassiveSearchProperties properties,
        TokenReportRepository repository
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
        return TokenReportColumns.HEADERS;
    }

    @Override
    protected long streamByKeys(CsvTemplate template, List<SearchInputRow> keys,
                                AnalysisWindow window, Consumer<TokenReportRow> consumer) {
        return repository.streamByKeys(template, keys, window, consumer);
    }
}
