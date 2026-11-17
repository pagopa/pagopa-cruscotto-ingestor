package it.pagopa.cruscotto.ingestion.fdr.storage;

/**
 * An entry found under the day prefix that is not a processable FDR file, with the reason why.
 *
 * <p>The reason is the point of this type. Collapsing every anomaly into "did not match" would make
 * a file dated the 16th sitting in the folder of the 15th indistinguishable from an unrelated
 * document someone dropped in the container — and those two call for opposite reactions. Discovery
 * only <em>classifies</em>: whether an anomaly fails the day, warns, or is ignored is a decision for
 * the orchestration, which is why nothing here throws.
 *
 * @param blobName the full blob path, kept verbatim so the entry can be found again
 * @param reason   why it was not accepted
 */
public record FdrInvalidEntry(String blobName, Reason reason) {

    /** Why an entry under the day prefix is not a processable FDR file. */
    public enum Reason {

        /** The name does not begin with either dataset prefix: it is not an FDR file at all. */
        UNKNOWN_DATASET,

        /**
         * It is an FDR file but does not end in {@code .csv.gz}. Covers {@code .csv} (uncompressed)
         * and {@code .zip}, which matters because earlier FDR documentation said ZIP: a file
         * actually arriving as ZIP is a change of contract, not a typo, and must surface as such.
         */
        WRONG_EXTENSION,

        /**
         * The name is well formed but carries a different day from the folder it sits in. Never
         * silently attributed to the folder's day: the file belongs to whoever produced it, and
         * guessing would corrupt a partition.
         */
        WRONG_DAY,

        /**
         * The progressive is not a strictly positive number: it contains something other than
         * digits ({@code -X}, {@code -1a}, {@code --1}), or it is zero ({@code -0}, {@code -000}),
         * which is not a position in a sequence.
         *
         * <p>Leading zeros are <strong>not</strong> a reason to reject: the customer never ruled
         * them out, so {@code -001} is a valid file and is returned. Nor is size — the number of
         * digits is unbounded, and a formally valid name must never become a technical failure of
         * the day because the value did not fit a Java type.
         */
        INVALID_PROGRESSIVE,

        /** Recognisable dataset and extension, but the middle does not match {@code <day>-<n>}. */
        MALFORMED_NAME,

        /**
         * The blob sits in a sub-folder of the day rather than directly in it. Azure's flat listing
         * returns everything under the prefix, so a nested path arrives here rather than being
         * filtered out by the service, and silently flattening it would process a file from a
         * structure nobody agreed on.
         */
        NESTED_PATH
    }
}
