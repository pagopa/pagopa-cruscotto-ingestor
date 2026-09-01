package it.pagopa.cruscotto.ingestion.massivesearch.report.position;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvInputReader;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvLineWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplateDetector;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.PerimeterCsvReader;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionContext;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises the shared {@code AbstractPerimeterReportGenerator} pipeline through a concrete generator:
 * header writing, perimeter de-duplication, batching and row streaming, with the repository mocked.
 */
class PositionReportGeneratorTest {

    private PositionReportRepository repository;
    private PositionReportGenerator generator;
    private final List<List<SearchInputRow>> capturedBatches = new ArrayList<>();

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        MassiveSearchProperties properties = new MassiveSearchProperties();
        properties.getExecution().setPerimeterBatchSize(2);
        repository = mock(PositionReportRepository.class);
        PerimeterCsvReader perimeterReader =
            new PerimeterCsvReader(new CsvInputReader(properties), new CsvTemplateDetector());
        CsvLineWriter lineWriter = new CsvLineWriter(properties);
        generator = new PositionReportGenerator(perimeterReader, lineWriter, properties, repository);

        when(repository.streamByKeys(any(), anyList(), any(), any())).thenAnswer(inv -> {
            List<SearchInputRow> keys = new ArrayList<>(inv.getArgument(1)); // batch is reused: copy it
            capturedBatches.add(keys);
            Consumer<PositionReportRow> consumer = inv.getArgument(3);
            for (SearchInputRow key : keys) {
                consumer.accept(new PositionReportRow(List.of(key.nav(), key.pa())));
            }
            return (long) keys.size();
        });
    }

    private MassiveSearchExecutionContext context(String content) {
        MassiveSearchExecutionContext ctx =
            new MassiveSearchExecutionContext(UUID.randomUUID(), UUID.randomUUID(), "CSV", false);
        ctx.setInputCsvContent(content);
        ctx.setInputTemplate(CsvTemplate.NAV_PA);
        ctx.setAnalysisWindow(AnalysisWindow.none());
        return ctx;
    }

    @Test
    void writesHeaderDeduplicatesAndStreamsRows() throws IOException {
        StringWriter out = new StringWriter();
        // (1,10) is duplicated in the perimeter: it must be queried and written exactly once.
        long rows = generator.writeReport(context("NAV,PA\n1,10\n2,20\n1,10\n3,30\n"), out);

        assertEquals(3, rows);

        List<SearchInputRow> queriedKeys = capturedBatches.stream().flatMap(List::stream).toList();
        assertEquals(3, queriedKeys.size());

        String output = out.toString();
        assertTrue(output.startsWith(String.join(",", PositionReportColumns.HEADERS) + "\r\n"));
        assertEquals(4, output.split("\r\n").length); // header + 3 data rows
    }

    @Test
    void throwsWhenInputContentMissing() {
        MassiveSearchExecutionContext ctx = context(null);
        assertThrows(MassiveSearchExecutionException.class, () -> generator.writeReport(ctx, new StringWriter()));
    }
}
