package it.pagopa.cruscotto.ingestion.fdr.persistence;

/**
 * The write cannot proceed, for a reason that is not one row's fault.
 *
 * <p>The counterpart of {@link FdrPersistenceError}: that one describes a row the database
 * refused and is counted, this one stops the file. The distinction is the same the retry policy
 * will need — rows are a data problem, this is a day-level problem worth attempting again in an
 * hour, or a configuration problem that no number of attempts will fix.
 */
public class FdrPersistenceException extends RuntimeException {

    private final transient Kind kind;

    public FdrPersistenceException(Kind kind, String message) {
        this(kind, message, null);
    }

    public FdrPersistenceException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    public enum Kind {

        /**
         * The business day has no explicit partition.
         *
         * <p>Configuration, not transient, and the one failure that exists because of a safety net:
         * both tables have a DEFAULT partition, so a day outside the 2026-2028 horizon would be
         * written <em>without any error</em> and land in a partition that cannot be pruned, grows
         * without bound, and that a reingestion's TRUNCATE would not touch. Failing before the
         * write is the whole point.
         */
        MISSING_PARTITION,

        /** The row the writer was handed does not belong to the dataset being written. */
        DATASET_MISMATCH
    }
}
