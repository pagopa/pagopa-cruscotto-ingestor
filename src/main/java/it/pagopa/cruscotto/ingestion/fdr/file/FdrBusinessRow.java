package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.LocalDate;

/**
 * What the two mapped rows have in common: the dataset they belong to and the day they are filed
 * under.
 *
 * <p>{@code dateInserted} is on the interface and not only on the records because it is the
 * partition key, and the one value in an FDR row that does <em>not</em> come from the file: it is
 * the day of the blob folder. Stating it here makes it hard to build a row without deciding where
 * it goes — which is the mistake that would spread a reingestion across today's partitions instead
 * of rewriting the original day.
 */
public interface FdrBusinessRow {

    /** The dataset, hence the table. */
    FdrDataset dataset();

    /** The FDR business day: the {@code yyyyMMdd} folder the file was found in, never {@code now()}. */
    LocalDate dateInserted();
}
