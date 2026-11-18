package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.LocalDate;

/**
 * One row the database refused.
 *
 * <p>Structured and not a log line, because the layer above will turn these into
 * {@code STG_INGEST_ERROR} rows through {@code insertDiscardedBulk} — the discarded path, never
 * {@code insertBulk}, whose {@code PENDING} state belongs to the ADX reconciliation that FDR does
 * not take part in. The writer stops at producing them: writing them from inside the same
 * transaction that is failing would tie the diagnosis to the fate of the data.
 *
 * <p>It carries no row payload. The file is still on the blob, the position says where to look,
 * and copying identifiers into an error object only spreads them into places nobody audited.
 *
 * @param rowOrdinal position of the row among those handed to the writer for this file, from 1.
 *                   Not the physical line of the CSV: rows the reader rejected never reach the
 *                   writer, so the two differ whenever parsing dropped something. It is a stable
 *                   reference within the write, which is what the diagnosis needs
 * @param sqlState   the five-character SQLSTATE, which is the one piece of the database error that
 *                   is both stable across versions and free of data
 * @param message    the driver's message, shortened. It can name a column and a limit, which is
 *                   the useful part; it is bounded so that one pathological value cannot write a
 *                   kilobyte into every record
 */
public record FdrPersistenceError(
        FdrDataset dataset,
        LocalDate businessDay,
        String blobName,
        long rowOrdinal,
        Category category,
        String sqlState,
        String message) {

    /** Longest message kept. Enough for "value too long for type character varying(35)". */
    static final int MAX_MESSAGE_LENGTH = 200;

    /** What kind of refusal it was, at the granularity that changes what one would do about it. */
    public enum Category {

        /** SQLSTATE class 22: the value does not fit the column, or cannot be converted to it. */
        DATA_ERROR,

        /** SQLSTATE class 23: a constraint rejected the row. */
        CONSTRAINT_VIOLATION,

        /** Isolable to a single row, but not one of the two classes above. */
        OTHER
    }

    static FdrPersistenceError of(FdrWriteContext context, long rowOrdinal, String sqlState,
                                  String message) {
        return new FdrPersistenceError(context.dataset(), context.businessDay(), context.blobName(),
                rowOrdinal, FdrSqlErrors.categoryOf(sqlState), sqlState,
                FdrSqlErrors.shorten(message, MAX_MESSAGE_LENGTH));
    }
}
