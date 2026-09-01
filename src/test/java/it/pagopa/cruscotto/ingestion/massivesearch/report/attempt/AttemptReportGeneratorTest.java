package it.pagopa.cruscotto.ingestion.massivesearch.report.attempt;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvInputReader;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvLineWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplateDetector;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.PerimeterCsvReader;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionContext;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.ReportType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies the attempt generator is wired to its own repository, header and type through the shared base. */
class AttemptReportGeneratorTest {

    private AttemptReportRepository repository;
    private AttemptReportGenerator generator;
    private final List<List<SearchInputRow>> capturedBatches = new ArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        MassiveSearchProperties properties = new MassiveSearchProperties();
        properties.getExecution().setPerimeterBatchSize(2);
        repository = mock(AttemptReportRepository.class);
        PerimeterCsvReader reader = new PerimeterCsvReader(new CsvInputReader(properties), new CsvTemplateDetector());
        generator = new AttemptReportGenerator(reader, new CsvLineWriter(properties), properties, repository);

        when(repository.streamByKeys(any(), anyList(), any(), any())).thenAnswer(inv -> {
            List<SearchInputRow> keys = new ArrayList<>(inv.getArgument(1));
            capturedBatches.add(keys);
            Consumer<AttemptReportRow> consumer = inv.getArgument(3);
            keys.forEach(k -> consumer.accept(new AttemptReportRow(List.of(k.nav(), k.pa()))));
            return (long) keys.size();
        });
    }

    @Test
    void writesHeaderDeduplicatesAndStreamsRows() throws IOException {
        MassiveSearchExecutionContext ctx =
            new MassiveSearchExecutionContext(UUID.randomUUID(), UUID.randomUUID(), "CSV", false);
        ctx.setInputCsvContent("NAV,PA\n1,10\n2,20\n1,10\n3,30\n"); // (1,10) duplicated
        ctx.setInputTemplate(CsvTemplate.NAV_PA);
        ctx.setAnalysisWindow(AnalysisWindow.none());

        StringWriter out = new StringWriter();
        long rows = generator.writeReport(ctx, out);

        assertEquals(ReportType.TOKEN, generator.type());
        assertEquals(3, rows);
        assertEquals(3, capturedBatches.stream().flatMap(List::stream).count());
        assertTrue(out.toString().startsWith(String.join(",", AttemptReportColumns.HEADERS) + "\r\n"));
    }
}
