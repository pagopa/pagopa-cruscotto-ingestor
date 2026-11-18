package it.pagopa.cruscotto.ingestion.fdr.file;

/**
 * One value of one row could not be mapped: a number that is not a number, a timestamp that is not
 * a timestamp.
 *
 * <p>Row-level, like {@link FdrMalformedLineException} and for the same reason — and equally
 * without a stack trace. It carries the column name because that is the question asked when the
 * warning shows up in production: not <em>how many</em> rows failed, which the counts answer, but
 * <em>which field</em> changed shape.
 */
public class FdrValueException extends RuntimeException {

    private final transient String column;

    public FdrValueException(String column, String message) {
        super(message, null, false, false);
        this.column = column;
    }

    public String column() {
        return column;
    }
}
