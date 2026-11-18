package it.pagopa.cruscotto.ingestion.fdr.file;

import java.time.LocalDate;
import java.util.List;

/**
 * Maps a line of {@code IUV_RENDICONTATI}. Stateless and shared, like its FLUSSI counterpart.
 */
public final class FdrIuvRowMapper implements FdrRowMapper<FdrIuvRow> {

    public static final FdrIuvRowMapper INSTANCE = new FdrIuvRowMapper();

    private FdrIuvRowMapper() {
    }

    @Override
    public FdrIuvRow map(FdrCsvHeader header, List<String> fields, LocalDate dateInserted) {
        return new FdrIuvRow(
                dateInserted,
                header.value(fields, FdrColumns.IUV),
                header.value(fields, FdrColumns.IUR),
                FdrValues.decimal(FdrColumns.IMPORTO, header.value(fields, FdrColumns.IMPORTO)),
                // COD_ESITO and IDSP have a DEFAULT 0 in the table but are mapped as nullable
                // Integer: the default covers the row that does not carry the column, null covers
                // the row that carries it empty. Substituting a zero here would make the two
                // indistinguishable, and zero is a meaningful outcome code.
                FdrValues.integer(FdrColumns.COD_ESITO, header.value(fields, FdrColumns.COD_ESITO)),
                FdrValues.timestamp(FdrColumns.DATA_ESITO_SINGOLO_PAGAMENTO,
                        header.value(fields, FdrColumns.DATA_ESITO_SINGOLO_PAGAMENTO)),
                FdrValues.integer(FdrColumns.IDSP, header.value(fields, FdrColumns.IDSP)),
                header.value(fields, FdrColumns.ID_FLUSSO),
                FdrValues.timestamp(FdrColumns.DATA_ORA_FLUSSO,
                        header.value(fields, FdrColumns.DATA_ORA_FLUSSO)),
                header.value(fields, FdrColumns.ID_DOMINIO),
                header.value(fields, FdrColumns.PSP),
                header.value(fields, FdrColumns.INT_PSP),
                header.value(fields, FdrColumns.UNIQUE_ID),
                FdrValues.timestamp(FdrColumns.INSERTED_TIMESTAMP,
                        header.value(fields, FdrColumns.INSERTED_TIMESTAMP)));
    }
}
