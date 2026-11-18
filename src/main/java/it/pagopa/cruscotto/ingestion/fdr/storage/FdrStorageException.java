package it.pagopa.cruscotto.ingestion.fdr.storage;

/**
 * A technical failure while talking to Blob Storage: the day could not be looked at.
 *
 * <p>One exception, a handful of kinds, no hierarchy. The distinction that matters is not between the many
 * ways Azure can fail — authorization, timeout, throttling, a broken connection — but between
 * <em>failing to look</em> and <em>looking and finding nothing</em>. The first throws, the second
 * returns an empty {@link FdrDayDiscovery}. Collapsing them would let a misconfigured container be
 * recorded as a quiet day with no files, and the day would be marked processed having read nothing.
 *
 * <p>{@link Kind#CONTAINER_NOT_FOUND} is split out because it is the one failure that is a
 * <em>configuration</em> problem rather than a transient one: retrying it five times an hour apart,
 * which is the policy for technical errors, would never help. The retry step will need to tell them
 * apart, and this is the seam that allows it.
 *
 * <p>Note that FDR deliberately does not create the container when missing, unlike the Massive
 * Search storage service which calls {@code createIfNotExists()}: that service owns its container
 * and writes to it, while FDR only reads one that someone else fills. Creating it would turn a
 * misconfiguration into an empty container and hide the problem for good.
 */
public class FdrStorageException extends RuntimeException {

    private final transient Kind kind;

    public FdrStorageException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    /** Whether the failure is a configuration problem or a failure of the call itself. */
    public enum Kind {

        /** The configured container does not exist. Retrying will not fix it. */
        CONTAINER_NOT_FOUND,

        /** Anything else while listing: authorization, timeout, throttling, transport, SDK. */
        LISTING_FAILED,

        /**
         * A blob that discovery had listed is no longer there when the read starts.
         *
         * <p>Kept apart from {@link #DOWNLOAD_FAILED} because it says something the generic failure
         * does not: the folder changed under us between listing and reading. Retrying the day is
         * the right answer — the next discovery will simply not list it — whereas retrying a
         * transport failure means asking Azure the same question again.
         */
        BLOB_NOT_FOUND,

        /** The blob exists but the stream could not be opened: authorization, timeout, transport. */
        DOWNLOAD_FAILED
    }
}
