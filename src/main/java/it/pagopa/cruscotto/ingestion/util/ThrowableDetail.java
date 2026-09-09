package it.pagopa.cruscotto.ingestion.util;

import java.sql.SQLException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Renders a throwable and <em>all</em> of its linked failures into a single self-describing line,
 * suitable for {@code INGEST_EXECUTION_LOG.ERROR_MESSAGE}.
 *
 * <p>Walking only {@link Throwable#getCause()} loses the actionable detail in two common cases:</p>
 * <ul>
 *   <li><b>JDBC batch</b> — {@link SQLException#getNextException()} holds the per-row error
 *       (e.g. {@code value too long}, {@code null value in column}); the batch exception itself only
 *       says "Call getNextException to see other errors in the batch".</li>
 *   <li><b>ADX / Kusto</b> — the top-level {@code DataServiceException} is generic
 *       ("...multiple inner exceptions") while the real signal
 *       (e.g. {@code LimitsExceeded} / {@code E_QUERY_RESULT_SET_TOO_LARGE}) lives in the inner
 *       exceptions the vendor exposes as an {@link Iterable}, not as a cause.</li>
 * </ul>
 *
 * <p>This formatter traverses cause, {@code getNextException()}, {@link Throwable#getSuppressed()}
 * and any {@link Iterable} of throwables, guarding against cycles and bounding both the number of
 * nodes and the total length so a pathological chain can never blow up the log row.</p>
 */
public final class ThrowableDetail {

    /** Upper bound on rendered nodes: enough to diagnose without letting an aggregate explode. */
    static final int MAX_NODES = 16;

    /** Hard cap on the rendered length; ERROR_MESSAGE is TEXT but a runaway chain stays bounded. */
    static final int MAX_LENGTH = 8000;

    private ThrowableDetail() {
    }

    /** Renders {@code t} and its linked failures; returns an empty string for {@code null}. */
    public static String format(Throwable t) {
        if (t == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        append(sb, t, "", seen, new int[] {0});
        return sb.toString();
    }

    private static void append(StringBuilder sb, Throwable t, String relation, Set<Throwable> seen, int[] count) {
        if (t == null || count[0] >= MAX_NODES || sb.length() >= MAX_LENGTH || !seen.add(t)) {
            return;
        }
        count[0]++;
        if (sb.length() > 0) {
            sb.append(" | ");
        }
        if (!relation.isEmpty()) {
            sb.append(relation).append('=');
        }
        sb.append(t.getClass().getSimpleName());
        String message = t.getMessage();
        if (message != null && !message.isBlank()) {
            sb.append(": ").append(message);
        }

        // Cause chain: the classic nesting.
        Throwable cause = t.getCause();
        if (cause != t) {
            append(sb, cause, "causedBy", seen, count);
        }
        // JDBC batch: the actionable per-row error is here, NOT in getCause().
        if (t instanceof SQLException sqlException) {
            SQLException next = sqlException.getNextException();
            if (next != t) {
                append(sb, next, "nextException", seen, count);
            }
        }
        // Vendor aggregates expose their inner failures NOT as a cause but via an accessor:
        //  - Kusto's KustoServiceQueryError#getExceptions() (verified against kusto-data 5.0.0: the
        //    LimitsExceeded / E_QUERY_RESULT_SET_TOO_LARGE detail lives here, never in getCause());
        //  - other aggregates simply implement Iterable.
        appendInnerExceptions(sb, t, seen, count);
        // Suppressed exceptions (e.g. from try-with-resources or aggregated failures).
        for (Throwable suppressed : t.getSuppressed()) {
            append(sb, suppressed, "suppressed", seen, count);
        }
    }

    /**
     * Surfaces inner exceptions carried out-of-band from the cause chain: a no-arg
     * {@code getExceptions()} returning a collection (reflection, so this util keeps no vendor
     * dependency) and any {@link Iterable} of throwables.
     */
    private static void appendInnerExceptions(StringBuilder sb, Throwable t, Set<Throwable> seen, int[] count) {
        try {
            Object inners = t.getClass().getMethod("getExceptions").invoke(t);
            appendThrowableElements(sb, inners, seen, count);
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // No getExceptions() accessor, or it failed: fall back to the Iterable shape below.
        }
        if (t instanceof Iterable<?> iterable) {
            appendThrowableElements(sb, iterable, seen, count);
        }
    }

    private static void appendThrowableElements(StringBuilder sb, Object candidate, Set<Throwable> seen, int[] count) {
        if (candidate instanceof Iterable<?> iterable) {
            for (Object element : iterable) {
                if (element instanceof Throwable inner) {
                    append(sb, inner, "inner", seen, count);
                }
            }
        }
    }
}
