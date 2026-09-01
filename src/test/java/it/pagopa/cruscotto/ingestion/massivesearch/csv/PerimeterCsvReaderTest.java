package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PerimeterCsvReaderTest {

    private final PerimeterCsvReader reader =
        new PerimeterCsvReader(new CsvInputReader(new MassiveSearchProperties()), new CsvTemplateDetector());

    /** Collects every emitted row (copying each reused batch) and records templates and batch sizes. */
    private List<SearchInputRow> collect(String content, CsvTemplate fallback, int batchSize,
                                         List<CsvTemplate> templatesOut, List<Integer> batchSizesOut) {
        List<SearchInputRow> all = new ArrayList<>();
        reader.forEachBatch(content, fallback, batchSize, (template, batch) -> {
            templatesOut.add(template);
            batchSizesOut.add(batch.size());
            all.addAll(new ArrayList<>(batch)); // batch is reused across calls: copy it
            return batch.size();
        });
        return all;
    }

    @Test
    void skipsDuplicateRows() {
        List<CsvTemplate> templates = new ArrayList<>();
        List<SearchInputRow> rows =
            collect("NAV,PA\n1,10\n2,20\n1,10\n3,30\n", null, 100, templates, new ArrayList<>());
        assertEquals(3, rows.size());
        assertEquals(CsvTemplate.NAV_PA, templates.get(0));
    }

    @Test
    void skipsBlankLines() {
        List<SearchInputRow> rows =
            collect("NAV,PA\n1,10\n\n   \n2,20\n", null, 100, new ArrayList<>(), new ArrayList<>());
        assertEquals(2, rows.size());
    }

    @Test
    void deduplicatesAcrossBatchBoundaries() {
        List<Integer> sizes = new ArrayList<>();
        // A,B fill the first batch; the second A is a duplicate and must be skipped, so C and D
        // form the second batch (a naive per-batch dedup would leak the duplicate into batch 2).
        List<SearchInputRow> rows =
            collect("NAV,PA\n1,10\n2,20\n1,10\n3,30\n4,40\n", null, 2, new ArrayList<>(), sizes);
        assertEquals(4, rows.size());
        assertEquals(List.of(2, 2), sizes);
    }

    @Test
    void emitsBoundedBatches() {
        List<Integer> sizes = new ArrayList<>();
        List<SearchInputRow> rows =
            collect("NAV,PA\n1,10\n2,20\n3,30\n4,40\n5,50\n", null, 2, new ArrayList<>(), sizes);
        assertEquals(5, rows.size());
        assertEquals(List.of(2, 2, 1), sizes);
    }

    @Test
    void returnsAccumulatedHandlerTotal() {
        long total = reader.forEachBatch("NAV,PA\n1,10\n2,20\n3,30\n", null, 2, (t, b) -> b.size());
        assertEquals(3, total);
    }

    @Test
    void returnsZeroAndNeverInvokesHandlerForEmptyHeaderOnlyOrNullInput() {
        AtomicInteger calls = new AtomicInteger();
        long empty = reader.forEachBatch("", null, 10, (t, b) -> { calls.incrementAndGet(); return b.size(); });
        long headerOnly = reader.forEachBatch("NAV,PA\n", null, 10, (t, b) -> { calls.incrementAndGet(); return b.size(); });
        long nullContent = reader.forEachBatch(null, null, 10, (t, b) -> { calls.incrementAndGet(); return b.size(); });
        assertEquals(0, empty);
        assertEquals(0, headerOnly);
        assertEquals(0, nullContent);
        assertEquals(0, calls.get());
    }

    @Test
    void usesFallbackTemplateWhenHeaderDoesNotResolveOne() {
        List<CsvTemplate> templates = new ArrayList<>();
        reader.forEachBatch("FOO\nx\n", CsvTemplate.IUV, 10, (t, b) -> { templates.add(t); return b.size(); });
        assertEquals(List.of(CsvTemplate.IUV), templates);
    }
}
