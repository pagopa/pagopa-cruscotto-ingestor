package it.pagopa.cruscotto.ingestion.fdr.file;

import java.time.LocalDate;
import java.util.List;

/**
 * Maps a line of {@code FLUSSI_RENDICONTAZIONE}.
 *
 * <p>Stateless, hence the single shared instance: a mapper per file would allocate one object per
 * blob for no reason, and there is nothing to keep between rows.
 */
public final class FdrFlussoRowMapper implements FdrRowMapper<FdrFlussoRow> {

    public static final FdrFlussoRowMapper INSTANCE = new FdrFlussoRowMapper();

    private FdrFlussoRowMapper() {
    }

    @Override
    public FdrFlussoRow map(FdrCsvHeader header, List<String> fields, LocalDate dateInserted) {
        return new FdrFlussoRow(
                dateInserted,
                FdrValues.timestamp(FdrColumns.INSERTED_TIMESTAMP,
                        header.value(fields, FdrColumns.INSERTED_TIMESTAMP)),
                header.value(fields, FdrColumns.ID_FLUSSO),
                FdrValues.timestamp(FdrColumns.DATA_ORA_FLUSSO,
                        header.value(fields, FdrColumns.DATA_ORA_FLUSSO)),
                // DATA_REGOLAMENTO is a full timestamp in the source even though it reads as a
                // date, and the column is TIMESTAMP to match. Mapping it to a LocalDate here would
                // drop a time component the source bothered to send.
                FdrValues.timestamp(FdrColumns.DATA_REGOLAMENTO,
                        header.value(fields, FdrColumns.DATA_REGOLAMENTO)),
                header.value(fields, FdrColumns.CAUSALE),
                FdrValues.integer(FdrColumns.NUM_PAGAMENTI,
                        header.value(fields, FdrColumns.NUM_PAGAMENTI)),
                FdrValues.decimal(FdrColumns.SOMMA_VERSATA,
                        header.value(fields, FdrColumns.SOMMA_VERSATA)),
                header.value(fields, FdrColumns.ID_DOMINIO),
                header.value(fields, FdrColumns.PSP),
                header.value(fields, FdrColumns.INT_PSP),
                header.value(fields, FdrColumns.UNIQUE_ID),
                header.value(fields, FdrColumns.ALL_DATES));
    }
}
