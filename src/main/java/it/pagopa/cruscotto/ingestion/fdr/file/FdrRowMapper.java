package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.LocalDate;
import java.util.List;

/**
 * Builds one business row out of one parsed CSV line.
 *
 * <p>Two implementations, one per dataset, both stateless and therefore shareable. The reader is
 * generic over the row type rather than returning a common supertype, so the persistence step gets
 * {@code FdrFlussoRow} or {@code FdrIuvRow} without a cast — a cast that would otherwise be the
 * only thing standing between the two tables.
 */
@FunctionalInterface
public interface FdrRowMapper<T extends FdrBusinessRow> {

    /**
     * @param header      the file's header, which resolves names to positions
     * @param fields      the fields of one data line
     * @param dateInserted the FDR day; it comes from the blob folder and is passed in because
     *                    nothing in the row carries it
     * @throws FdrValueException when a value cannot be mapped. The reader counts the row as
     *                           rejected and carries on with the next one
     */
    T map(FdrCsvHeader header, List<String> fields, LocalDate dateInserted);

    /** The mapper for a dataset. */
    static FdrRowMapper<? extends FdrBusinessRow> forDataset(FdrDataset dataset) {
        return dataset == FdrDataset.FLUSSI_RENDICONTAZIONE
                ? FdrFlussoRowMapper.INSTANCE
                : FdrIuvRowMapper.INSTANCE;
    }
}
