package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFlussoRow;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The INSERT of {@code FDR_FLUSSI_RENDICONTAZIONE}.
 *
 * <p>The {@code ID} comes from the sequence inside the statement, as the AS-IS bulk writer does
 * for every entity: one round trip for the whole batch, no counter in memory to get out of step
 * with the database, and nothing to reconcile after a failed batch.
 *
 * <p>Two things this statement deliberately does <em>not</em> have:
 * <ul>
 *   <li>no {@code ON CONFLICT}. The real files contain exact duplicate rows — 14 in one day's
 *       flows — and the requirement is that they arrive and can be seen. A silent
 *       {@code DO NOTHING} would make a source defect disappear into a count nobody compares;</li>
 *   <li>no default for a missing value. {@code COD_ESITO} and {@code IDSP} have a DEFAULT 0 in the
 *       DDL, but every column is bound explicitly, so an absent value is written as SQL
 *       {@code NULL} and stays distinguishable from a real zero.</li>
 * </ul>
 *
 * <p>{@code CAUSALE} arrives here already shortened by {@link FdrCausale}; this class performs no
 * transformation of its own, which is what keeps the authorised truncation in one place.
 * {@code ALL_DATES} is written as the raw source text into a {@code TEXT} column: its business
 * meaning is not settled, so nothing parses, sorts or reshapes it.
 */
final class FdrFlussoInsert implements FdrInsertStatement {

    static final FdrFlussoInsert INSTANCE = new FdrFlussoInsert();

    private FdrFlussoInsert() {
    }

    @Override
    public FdrDataset dataset() {
        return FdrDataset.FLUSSI_RENDICONTAZIONE;
    }

    @Override
    public String sql(String schema) {
        return "INSERT INTO " + schema + ".FDR_FLUSSI_RENDICONTAZIONE "
                + "(ID, DATE_INSERTED, INSERTED_TIMESTAMP, ID_FLUSSO, DATA_ORA_FLUSSO, "
                + "DATA_REGOLAMENTO, CAUSALE, NUM_PAGAMENTI, SOMMA_VERSATA, ID_DOMINIO, PSP, "
                + "INT_PSP, UNIQUE_ID, ALL_DATES) "
                + "VALUES (nextval('" + schema + ".SQ_FDR_FLUSSI_RENDICONTAZIONE'), "
                + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    @Override
    public void bind(PreparedStatement ps, FdrBusinessRow row) throws SQLException {
        FdrFlussoRow flusso = (FdrFlussoRow) row;
        FdrInsertStatement.setBusinessDay(ps, 1, flusso.dateInserted());
        FdrInsertStatement.setTimestamp(ps, 2, flusso.insertedTimestamp());
        ps.setString(3, flusso.idFlusso());
        FdrInsertStatement.setTimestamp(ps, 4, flusso.dataOraFlusso());
        FdrInsertStatement.setTimestamp(ps, 5, flusso.dataRegolamento());
        ps.setString(6, flusso.causale());
        FdrInsertStatement.setInteger(ps, 7, flusso.numPagamenti());
        FdrInsertStatement.setDecimal(ps, 8, flusso.sommaVersata());
        ps.setString(9, flusso.idDominio());
        ps.setString(10, flusso.psp());
        ps.setString(11, flusso.intPsp());
        ps.setString(12, flusso.uniqueId());
        ps.setString(13, flusso.allDates());
    }
}
