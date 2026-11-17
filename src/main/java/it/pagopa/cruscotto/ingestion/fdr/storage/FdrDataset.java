package it.pagopa.cruscotto.ingestion.fdr.storage;

import java.util.Optional;

/**
 * The two FDR datasets published daily on Blob Storage.
 *
 * <p>This is an FDR concept and deliberately <em>not</em> an {@code EntityName}: that enum drives
 * the ADX ingestion path — query builders, checkpoints, transformers — and FDR crosses none of it.
 *
 * <p><strong>The constant name and the file prefix are the same string today, and that is a
 * coincidence worth keeping visible.</strong> The constant is a value we own: it is what goes into
 * {@code FDR_INGESTION_STATUS.DATASET} and what the workflow dispatches on. The prefix belongs to
 * the customer, who names the files. The database tables are already a third thing — they carry the
 * {@code FDR_} prefix that the files do not. Keeping {@link #filePrefix} explicit means that the day
 * the customer renames a file, only this field changes.
 */
public enum FdrDataset {

    /** Flow revisions. Files are {@code FLUSSI_RENDICONTAZIONE_<yyyyMMdd>-<n>.csv.gz}. */
    FLUSSI_RENDICONTAZIONE("FLUSSI_RENDICONTAZIONE"),

    /** Reported transfers. Files are {@code IUV_RENDICONTATI_<yyyyMMdd>-<n>.csv.gz}. */
    IUV_RENDICONTATI("IUV_RENDICONTATI");

    private final String filePrefix;

    FdrDataset(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    /** The filename prefix, without the trailing separator. */
    public String filePrefix() {
        return filePrefix;
    }

    /**
     * The dataset whose prefix opens the given file name, if any.
     *
     * <p>Matches on {@code prefix + '_'} and not on the bare prefix, so that a dataset name which
     * is a strict prefix of another could never swallow it. The two current names are unrelated, so
     * today it changes nothing — it is the kind of guard that costs one character and removes a
     * whole class of future mistake.
     *
     * <p>Note what this does <em>not</em> do: a name such as
     * {@code IUV_RENDICONTATI_EXTRA_20260915-1.csv.gz} still matches, because it genuinely starts
     * with the dataset prefix. It is then rejected further down as a malformed FDR name, which is
     * the right outcome: a file announcing itself as ours and failing the contract is a different
     * problem from a document that was never ours, and deserves to be reported as such.
     */
    public static Optional<FdrDataset> matching(String fileName) {
        if (fileName == null) {
            return Optional.empty();
        }
        for (FdrDataset dataset : values()) {
            if (fileName.startsWith(dataset.filePrefix + '_')) {
                return Optional.of(dataset);
            }
        }
        return Optional.empty();
    }
}
