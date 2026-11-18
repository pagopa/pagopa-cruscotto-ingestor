package it.pagopa.cruscotto.ingestion.fdr.file;

/**
 * One line could not be split into fields.
 *
 * <p>Row-level, not file-level: the reader records it and moves on to the next line, because a
 * single corrupt line must not cost a day of data. The counterpart that does stop a file is
 * {@link FdrFileFormatException}.
 *
 * <p><strong>No stack trace, on purpose.</strong> This exception is control flow over a file that
 * can hold 600.000 rows, and filling in a stack trace for each one would turn a corrupt file into a
 * performance incident. The trace would also say nothing: the interesting context is the line
 * number and the reason, both of which the reader records in {@link FdrRowRejection}.
 */
public class FdrMalformedLineException extends RuntimeException {

    public FdrMalformedLineException(String message) {
        super(message, null, false, false);
    }
}
