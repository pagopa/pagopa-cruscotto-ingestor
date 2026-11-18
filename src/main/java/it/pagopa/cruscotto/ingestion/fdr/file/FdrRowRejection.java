package it.pagopa.cruscotto.ingestion.fdr.file;

/**
 * One data row that could not be turned into a business row.
 *
 * <p>Carries the physical line number, the column when there is one, and a short reason. It
 * deliberately does <strong>not</strong> carry the whole line: an FDR line holds identifiers and
 * amounts, and copying it into a diagnostic object — which ends up in a log — would put source data
 * where nobody decided to put it. The line number is enough to go and look at the file, which is
 * still on the blob.
 *
 * <p>The {@code detail} does include the offending value, and that is a considered exception rather
 * than an oversight: only the typed columns can fail — amounts, counters, timestamps — and none of
 * them is an identifier, let alone one of the values the logging standard forbids. Knowing that a
 * timestamp arrived as {@code 2026-09-15 08:38:01} instead of ISO-8601 is the difference between
 * fixing the mapping in an afternoon and asking the customer for a sample. It is written at DEBUG,
 * and only for a bounded sample of rows.
 *
 * @param lineNumber physical line in the decompressed file, header counted as line 1, so that it
 *                   matches what a text editor shows
 * @param column     the column that failed, or {@code null} when the whole line did
 * @param reason     the classification, used for the counters
 * @param detail     short human-readable text
 */
public record FdrRowRejection(long lineNumber, String column, Reason reason, String detail) {

    /** Why a row was rejected. Few and stable: these become counters on a log line. */
    public enum Reason {

        /** The line could not be split: an unterminated quoted field. */
        MALFORMED_LINE,

        /** The line has fewer fields than the header declares. */
        FIELD_COUNT_MISMATCH,

        /** A value is not of the type its column holds: a number, a timestamp. */
        INVALID_VALUE
    }

    static FdrRowRejection malformedLine(long lineNumber, String detail) {
        return new FdrRowRejection(lineNumber, null, Reason.MALFORMED_LINE, detail);
    }

    /**
     * Built from a mapping failure, separating a short row from a bad value.
     *
     * <p>The two arrive as the same exception because both are detected while reading a column, but
     * they mean different things in production: one says the file is damaged, the other that a
     * field changed shape. Collapsing them into one counter would hide whichever is rarer.
     */
    static FdrRowRejection fromValue(long lineNumber, FdrValueException e, int expectedColumns,
                                     int actualFields) {
        Reason reason = actualFields < expectedColumns
                ? Reason.FIELD_COUNT_MISMATCH
                : Reason.INVALID_VALUE;
        return new FdrRowRejection(lineNumber, e.column(), reason, e.getMessage());
    }
}
