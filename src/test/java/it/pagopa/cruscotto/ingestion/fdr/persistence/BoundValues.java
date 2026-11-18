package it.pagopa.cruscotto.ingestion.fdr.persistence;

import org.mockito.Mockito;
import org.mockito.invocation.Invocation;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Cosa finisce davvero nei parametri del PreparedStatement.
 *
 * <p>Il modo piu' diretto per verificare un mapping JDBC senza un database: si lascia che il
 * binding giri su un PreparedStatement finto e si rileggono le invocazioni. Senza questo, i test
 * del mapping verificherebbero il codice del test invece di quello di produzione.
 */
final class BoundValues {

    private BoundValues() {
    }

    /** I valori bindati per una riga, indicizzati per posizione del parametro. */
    static Map<Integer, Object> of(FdrInsertStatement statement,
                                   it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow row)
            throws SQLException {
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        statement.bind(ps, row);
        return read(ps);
    }

    /** I valori bindati dalla riga {@code index} di un batch. */
    static Map<Integer, Object> of(BatchPreparedStatementSetter setter, int index)
            throws SQLException {
        PreparedStatement ps = Mockito.mock(PreparedStatement.class);
        setter.setValues(ps, index);
        return read(ps);
    }

    private static Map<Integer, Object> read(PreparedStatement ps) {
        Map<Integer, Object> values = new HashMap<>();
        for (Invocation invocation : Mockito.mockingDetails(ps).getInvocations()) {
            Object[] arguments = invocation.getArguments();
            // setString(i, v), setBigDecimal(i, v), setObject(i, v) e setObject(i, v, type)
            // condividono la forma: il primo argomento e' l'indice, il secondo il valore.
            if (arguments.length >= 2 && arguments[0] instanceof Integer index) {
                values.put(index, arguments[1]);
            }
        }
        return values;
    }
}
