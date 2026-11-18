package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.LocalDate;
import java.util.Objects;

/**
 * Where the rows being written came from.
 *
 * <p>Three values, and the important one is {@code businessDay}: it is the authoritative source of
 * {@code DATE_INSERTED}, and it is passed in rather than derived so that no part of the writer can
 * reach for a clock or read a date out of the data. {@code ID_FLUSSO} famously starts with a date
 * in one PSP's files — that date is a naming convention of theirs, not the FDR day, and nothing
 * here may use it.
 *
 * <p>The other two are diagnostic: they turn a row rejection into something a person can act on
 * without going back through the logs to find out which file it came from.
 */
public record FdrWriteContext(FdrDataset dataset, LocalDate businessDay, String blobName) {

    public FdrWriteContext {
        Objects.requireNonNull(dataset, "dataset");
        Objects.requireNonNull(businessDay, "businessDay");
    }

    /** The context of a file found by discovery. The day is the folder's, as everywhere in FDR. */
    public static FdrWriteContext of(FdrBlobFile file) {
        Objects.requireNonNull(file, "file");
        return new FdrWriteContext(file.dataset(), file.businessDay(), file.blobName());
    }
}
