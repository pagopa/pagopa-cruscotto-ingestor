package it.pagopa.cruscotto.ingestion.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import org.junit.jupiter.api.Test;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThrowableDetailTest {

    @Test
    void nullReturnsEmptyString() {
        assertEquals("", ThrowableDetail.format(null));
    }

    @Test
    void rendersCauseChain() {
        Throwable t = new RuntimeException("wrapper", new IllegalStateException("root"));
        String out = ThrowableDetail.format(t);
        assertTrue(out.startsWith("RuntimeException: wrapper"), out);
        assertTrue(out.contains("causedBy=IllegalStateException: root"), out);
    }

    @Test
    void capturesSqlBatchNextException() {
        // JDBC batch: the actionable per-row error is in getNextException(), not getCause().
        BatchUpdateException batch = new BatchUpdateException(
                "Batch entry 6 ... was aborted: Call getNextException to see other errors in the batch.",
                new int[] {});
        batch.setNextException(new SQLException("ERROR: value too long for type character varying(255)"));
        Throwable wrapper = new RuntimeException("Bulk write failed", batch);

        String out = ThrowableDetail.format(wrapper);

        assertTrue(out.contains("nextException=SQLException: ERROR: value too long for type character varying(255)"),
                out);
    }

    @Test
    void capturesIterableInnerExceptions() {
        // Kusto exposes its "multiple inner exceptions" as an Iterable, not as a cause.
        MultiInnerException kusto = new MultiInnerException(
                "Query execution failed with multiple inner exceptions",
                List.of(new IllegalStateException("E_QUERY_RESULT_SET_TOO_LARGE")));

        String out = ThrowableDetail.format(kusto);

        assertTrue(out.contains("inner=IllegalStateException: E_QUERY_RESULT_SET_TOO_LARGE"), out);
    }

    @Test
    void surfacesKustoInnerExceptionsFromRealSdkClass() throws Exception {
        // Ground truth against kusto-data 5.0.0: KustoServiceQueryError does NOT implement Iterable and
        // has no cause — the LimitsExceeded / E_QUERY_RESULT_SET_TOO_LARGE detail is reachable ONLY via
        // getExceptions(). This is what the window-too-large classifier must see to trigger halving.
        ObjectMapper mapper = new ObjectMapper();
        ArrayNode innerExceptions = mapper.createArrayNode();
        innerExceptions.add(mapper.readTree(
                "{\"error\":{\"code\":\"LimitsExceeded\",\"@message\":\"The results of this query exceed the "
                        + "set limit of 64 MB (E_QUERY_RESULT_SET_TOO_LARGE, 0x80DA0003).\"}}"));
        KustoServiceQueryError kustoError = new KustoServiceQueryError(
                innerExceptions, false, "Query execution failed with multiple inner exceptions");
        Throwable top = new RuntimeException(
                "Error found while parsing json response as KustoOperationResult:"
                        + "Query execution failed with multiple inner exceptions", kustoError);

        String out = ThrowableDetail.format(top);

        assertTrue(out.contains("inner="), out);
        assertTrue(out.contains("LimitsExceeded"), out);
        assertTrue(out.contains("E_QUERY_RESULT_SET_TOO_LARGE"), out);
    }

    @Test
    void capturesSuppressedExceptions() {
        Throwable t = new RuntimeException("main");
        t.addSuppressed(new IllegalArgumentException("side"));

        String out = ThrowableDetail.format(t);

        assertTrue(out.contains("suppressed=IllegalArgumentException: side"), out);
    }

    @Test
    void toleratesCyclesWithoutLooping() {
        Throwable a = new RuntimeException("a");
        Throwable b = new RuntimeException("b", a);
        a.initCause(b); // a -> b -> a cycle

        String out = ThrowableDetail.format(a);

        // Each node is rendered once; the cycle does not blow up or repeat unboundedly.
        assertEquals(1, countOccurrences(out, "RuntimeException: a"), out);
        assertEquals(1, countOccurrences(out, "RuntimeException: b"), out);
    }

    @Test
    void boundsRenderedNodes() {
        // A long linear chain must stop at MAX_NODES rather than render everything.
        Throwable head = new RuntimeException("n0");
        Throwable current = head;
        for (int i = 1; i < ThrowableDetail.MAX_NODES + 10; i++) {
            Throwable next = new RuntimeException("n" + i);
            current.initCause(next);
            current = next;
        }
        String out = ThrowableDetail.format(head);
        assertEquals(ThrowableDetail.MAX_NODES, countOccurrences(out, "RuntimeException: n"), out);
        assertFalse(out.contains("n" + (ThrowableDetail.MAX_NODES + 5)), out);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) >= 0) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    /** Minimal stand-in for a vendor exception (e.g. Kusto) that exposes inner failures as an Iterable. */
    private static final class MultiInnerException extends RuntimeException implements Iterable<Throwable> {
        private final transient List<Throwable> inner;

        MultiInnerException(String message, List<Throwable> inner) {
            super(message);
            this.inner = inner;
        }

        @Override
        public Iterator<Throwable> iterator() {
            return inner.iterator();
        }
    }
}
