package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * The INSERT of one dataset: its SQL, and how a row binds to it.
 *
 * <p>Two implementations, chosen by dataset through {@link #forDataset}. The dispatch is static
 * and exhaustive on purpose — a table name is never built from anything that came in from outside,
 * and the SQL is a constant with bind parameters, so there is nothing for a value to escape into.
 *
 * <p>The binding helpers below exist because the three ways JDBC can lose information are all
 * reachable by accident:
 * <ul>
 *   <li>a {@code null} {@code Integer} unboxed into {@code setInt} becomes 0, and 0 is a valid
 *       {@code COD_ESITO}. {@link #setInteger} passes the type instead;</li>
 *   <li>a timestamp passed through a time zone moves by an hour twice a year. There is no
 *       conversion at all here: the reader produced a {@link LocalDateTime} — the reading as the
 *       file wrote it — and that is what {@link #setTimestamp} binds, which is exactly what a
 *       {@code TIMESTAMP WITHOUT TIME ZONE} column holds;</li>
 *   <li>an amount passed through {@code double} loses the scale the source wrote. Amounts are
 *       bound as {@link BigDecimal} and never converted.</li>
 * </ul>
 */
interface FdrInsertStatement {

    FdrDataset dataset();

    /** The INSERT, with the schema resolved. Constant text, bind parameters only. */
    String sql(String schema);

    /**
     * Binds one row.
     *
     * <p>The row is typed as {@link FdrBusinessRow} and cast inside: the session checks every
     * row's dataset before a batch is prepared, so the cast cannot fail without that check having
     * failed first — and that check raises a named exception rather than a {@code ClassCastException}.
     */
    void bind(PreparedStatement ps, FdrBusinessRow row) throws SQLException;

    static FdrInsertStatement forDataset(FdrDataset dataset) {
        return dataset == FdrDataset.FLUSSI_RENDICONTAZIONE
                ? FdrFlussoInsert.INSTANCE
                : FdrIuvInsert.INSTANCE;
    }

    /** {@code DATE_INSERTED}: a plain calendar day, bound without any time zone taking part. */
    static void setBusinessDay(PreparedStatement ps, int index, LocalDate day) throws SQLException {
        ps.setObject(index, day == null ? null : Date.valueOf(day), Types.DATE);
    }

    /**
     * The source reading, bound as it is.
     *
     * <p>No {@code Instant}, no {@code Calendar}, no zone: the value that reaches the column must
     * be the one that is in the file, digit for digit. {@code setObject} with a
     * {@link LocalDateTime} is the one form the driver writes without consulting a time zone —
     * {@code setTimestamp(Timestamp)} would leave room for one.
     */
    static void setTimestamp(PreparedStatement ps, int index, LocalDateTime value)
            throws SQLException {
        ps.setObject(index, value, Types.TIMESTAMP);
    }

    static void setInteger(PreparedStatement ps, int index, Integer value) throws SQLException {
        ps.setObject(index, value, Types.INTEGER);
    }

    static void setDecimal(PreparedStatement ps, int index, BigDecimal value) throws SQLException {
        ps.setBigDecimal(index, value);
    }
}
