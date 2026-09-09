package it.pagopa.cruscotto.ingestion.util;

/**
 * Deterministic, surrogate-safe truncation of text values to the width of a DB column.
 *
 * <p>ADX can carry malformed free-text values longer than the target {@code VARCHAR(n)} column.
 * Without clamping, the INSERT fails with {@code value too long for type character varying(n)};
 * because a SQL failure during ingestion is fail-fast, the whole run aborts on that single row,
 * the checkpoint is never persisted, and the next run re-reads the same row and fails again — one
 * dirty value blocks the entity (and its children) indefinitely.</p>
 *
 * <p>Truncation is deterministic, so cache, SELECT and INSERT keep resolving to the same key.
 * Callers own their own once-per-value logging; this helper is a pure function.</p>
 */
public final class ColumnValueClamp {

    /** Width of the standard {@code VARCHAR(255)} text columns across the ingestor schema. */
    public static final int VARCHAR_255 = 255;

    private ColumnValueClamp() {
    }

    /**
     * Returns {@code value} truncated to at most {@code maxLength} UTF-16 code units, never splitting
     * a surrogate pair (a lone surrogate would make PostgreSQL reject the INSERT with
     * "invalid byte sequence for encoding UTF8" — the very failure this clamp prevents).
     * {@code null} and values already within the limit are returned unchanged.
     */
    public static String clamp(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        int end = maxLength;
        if (end > 0 && Character.isHighSurrogate(value.charAt(end - 1))) {
            end--;
        }
        return value.substring(0, end);
    }
}
