package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrIuvRow;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The INSERT of {@code FDR_IUV_RENDICONTATI}.
 *
 * <p>Same shape as its FLUSSI counterpart, and the same two absences: no {@code ON CONFLICT} and
 * no default substituted for a missing value. The functional key of a transfer inside a revision
 * is {@code IUV + IUR + IDSP}, and there is deliberately no unique constraint on it — an anomalous
 * source must be ingested and diagnosed, not rejected — so duplicates land as separate rows here.
 *
 * <p><strong>{@code IUV} is a string and must stay one.</strong> It arrives zero-padded
 * ({@code 00000000002043975}) and can be alphanumeric, {@code RF...} included. Any numeric
 * conversion would eat the leading zeros and break every search the backend is built on; the
 * column is {@code VARCHAR} and the binding is {@code setString}, with no normalisation in
 * between.
 */
final class FdrIuvInsert implements FdrInsertStatement {

    static final FdrIuvInsert INSTANCE = new FdrIuvInsert();

    private FdrIuvInsert() {
    }

    @Override
    public FdrDataset dataset() {
        return FdrDataset.IUV_RENDICONTATI;
    }

    @Override
    public String sql(String schema) {
        return "INSERT INTO " + schema + ".FDR_IUV_RENDICONTATI "
                + "(ID, DATE_INSERTED, IUV, IUR, IMPORTO, COD_ESITO, DATA_ESITO_SINGOLO_PAGAMENTO, "
                + "IDSP, ID_FLUSSO, DATA_ORA_FLUSSO, ID_DOMINIO, PSP, INT_PSP, UNIQUE_ID, "
                + "INSERTED_TIMESTAMP) "
                + "VALUES (nextval('" + schema + ".SQ_FDR_IUV_RENDICONTATI'), "
                + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public void bind(PreparedStatement ps, FdrBusinessRow row) throws SQLException {
        FdrIuvRow iuv = (FdrIuvRow) row;
        FdrInsertStatement.setBusinessDay(ps, 1, iuv.dateInserted());
        ps.setString(2, iuv.iuv());
        ps.setString(3, iuv.iur());
        FdrInsertStatement.setDecimal(ps, 4, iuv.importo());
        FdrInsertStatement.setInteger(ps, 5, iuv.codEsito());
        FdrInsertStatement.setTimestamp(ps, 6, iuv.dataEsitoSingoloPagamento());
        FdrInsertStatement.setInteger(ps, 7, iuv.idsp());
        ps.setString(8, iuv.idFlusso());
        FdrInsertStatement.setTimestamp(ps, 9, iuv.dataOraFlusso());
        ps.setString(10, iuv.idDominio());
        ps.setString(11, iuv.psp());
        ps.setString(12, iuv.intPsp());
        ps.setString(13, iuv.uniqueId());
        FdrInsertStatement.setTimestamp(ps, 14, iuv.insertedTimestamp());
    }
}
