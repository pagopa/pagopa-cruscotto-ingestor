package it.pagopa.cruscotto.ingestion.fdr.file;

/**
 * The file as a whole cannot be read: it is not what it claims to be.
 *
 * <p>File-level, as opposed to the row-level {@link FdrMalformedLineException} and
 * {@link FdrValueException}. The distinction decides what the workflow does with the day: a handful
 * of bad rows leaves a dataset {@code COMPLETED_WITH_WARNING}, while a file that cannot be opened
 * or whose header does not match leaves nothing to count and must fail.
 *
 * <p>The {@link Kind} exists so the retry step can tell apart what is worth attempting again. A
 * truncated download is transient; a header missing a column is a change of contract, and five
 * retries an hour apart would read the same wrong header five times.
 */
public class FdrFileFormatException extends RuntimeException {

    private final transient Kind kind;

    public FdrFileFormatException(Kind kind, String message) {
        this(kind, message, null);
    }

    public FdrFileFormatException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    public Kind kind() {
        return kind;
    }

    /** Whether reading the same bytes again could possibly succeed. */
    public enum Kind {

        /**
         * The stream is not GZIP, or the GZIP stream is truncated or corrupt.
         *
         * <p>Retryable: a download cut short produces exactly this, and the blob on the storage is
         * usually intact.
         */
        CORRUPT_ARCHIVE,

        /**
         * The decompressed bytes are not valid UTF-8.
         *
         * <p>Reported rather than replaced. The decoder could substitute U+FFFD for every bad byte
         * and carry on, which is its default, but that turns a damaged download into a file of
         * plausible-looking rows with silently mangled identifiers. An IUV with a replacement
         * character in it is not an IUV, and nothing downstream would notice.
         */
        INVALID_ENCODING,

        /** The file is empty: not even a header line. Distinct from a file with a header and no rows. */
        MISSING_HEADER,

        /**
         * The header does not match the dataset: a column is missing, or appears twice.
         *
         * <p>Not retryable. This is the signal that the source contract changed, and the dataset
         * must stop while the files are still on the blob.
         */
        UNEXPECTED_HEADER,

        /** The stream failed while being read: transport, timeout, a closed connection. Retryable. */
        READ_FAILED
    }
}
