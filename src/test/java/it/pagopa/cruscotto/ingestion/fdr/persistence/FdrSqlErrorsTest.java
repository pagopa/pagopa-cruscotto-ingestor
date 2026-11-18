package it.pagopa.cruscotto.ingestion.fdr.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessResourceException;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * La classificazione da cui dipende tutto il fallback.
 *
 * <p>Dividere un batch e' la risposta giusta a un valore che non entra in una colonna e quella
 * sbagliata a una connessione chiusa: durante un disservizio del database uno split ricorsivo
 * trasforma un batch fallito in migliaia di tentativi inutili e seppellisce la causa vera.
 */
class FdrSqlErrorsTest {

    /** Classe 22 - data exception: valore troppo lungo, testo non convertibile, out of range. */
    @ParameterizedTest
    @ValueSource(strings = {"22001", "22P02", "22003"})
    void gliErroriSuiDatiSonoIsolabili(String sqlState) {
        assertThat(FdrSqlErrors.isRowIsolable(dataAccess(sqlState))).isTrue();
    }

    /** Classe 23 - violazione di vincolo: su FDR solo la PK, quindi comunque una riga sola. */
    @Test
    void laViolazioneDiVincoloEIsolabile() {
        assertThat(FdrSqlErrors.isRowIsolable(dataAccess("23505"))).isTrue();
    }

    /**
     * Connessione, risorse esaurite, intervento operatore, rollback di transazione, regola di
     * accesso (che comprende tabella o partizione mancante), autorizzazione. Nessuno di questi
     * migliora dividendo il batch.
     */
    @ParameterizedTest
    @ValueSource(strings = {"08006", "08003", "53100", "57014", "40001", "42P01", "28000", "3D000"})
    void gliErroriTecniciNonSonoIsolabili(String sqlState) {
        assertThat(FdrSqlErrors.isRowIsolable(dataAccess(sqlState))).isFalse();
    }

    /**
     * Nessun SQLSTATE significa che il guasto non viene dal server: e' la firma di un problema di
     * trasporto o di driver. Va trattato come tecnico, che e' la direzione in cui sbagliare costa
     * un ritardo e non una tempesta di tentativi.
     */
    @Test
    void senzaSqlstateSiAssumeTecnico() {
        assertThat(FdrSqlErrors.isRowIsolable(new TransientDataAccessResourceException("boom")))
                .isFalse();
        assertThat(FdrSqlErrors.isRowIsolable(new IllegalStateException("boom"))).isFalse();
    }

    @Test
    void loSqlstateVieneCercatoLungoTuttaLaCatenaDelleCause() {
        RuntimeException wrapped = new IllegalStateException("outer",
                new DataIntegrityViolationException("inner",
                        new SQLException("value too long", "22001")));

        assertThat(FdrSqlErrors.sqlStateOf(wrapped)).isEqualTo("22001");
        assertThat(FdrSqlErrors.isRowIsolable(wrapped)).isTrue();
    }

    @Test
    void laCategoriaSegueLaClasseDelloSqlstate() {
        assertThat(FdrSqlErrors.categoryOf("22001"))
                .isEqualTo(FdrPersistenceError.Category.DATA_ERROR);
        assertThat(FdrSqlErrors.categoryOf("23505"))
                .isEqualTo(FdrPersistenceError.Category.CONSTRAINT_VIOLATION);
        assertThat(FdrSqlErrors.categoryOf("08006"))
                .isEqualTo(FdrPersistenceError.Category.OTHER);
        assertThat(FdrSqlErrors.categoryOf(null)).isEqualTo(FdrPersistenceError.Category.OTHER);
    }

    /**
     * Il messaggio del driver cita volentieri il valore che ha rifiutato: va accorciato, perche' un
     * record di errore non e' il posto dove conservare una copia del dato che lo ha causato. E su
     * una riga sola, altrimenti romperebbe il JSON a riga singola del logging.
     */
    /**
     * Il messaggio da tenere e' quello del driver, non quello dell'involucro Spring. La traduzione
     * sostituisce il testo con la propria classificazione - "rejected", "could not execute
     * statement" - mentre il driver dice quale colonna e quale limite: e' la differenza fra una
     * diagnosi di cinque minuti e una richiesta di campioni al cliente.
     */
    @Test
    void ilMessaggioTenutoEQuelloDelDatabaseNonQuelloDellInvolucro() {
        RuntimeException wrapped = new DataIntegrityViolationException("rejected",
                new SQLException("value too long for type character varying(30)", "22001"));

        assertThat(FdrSqlErrors.messageOf(wrapped))
                .isEqualTo("value too long for type character varying(30)");
    }

    @Test
    void senzaSqlExceptionRestaIlMessaggioDellEccezione() {
        assertThat(FdrSqlErrors.messageOf(new IllegalStateException("pool exhausted")))
                .isEqualTo("pool exhausted");
        assertThat(FdrSqlErrors.messageOf(null)).isNull();
    }

    @Test
    void ilMessaggioVieneAccorciatoEMessoSuUnaRigaSola() {
        assertThat(FdrSqlErrors.shorten("riga uno\nriga due\r\nriga tre", 100))
                .isEqualTo("riga uno riga due  riga tre");
        assertThat(FdrSqlErrors.shorten("x".repeat(500), 200)).hasSize(200);
        assertThat(FdrSqlErrors.shorten(null, 200)).isNull();
    }

    private static RuntimeException dataAccess(String sqlState) {
        SQLException cause = new SQLException("db says no", sqlState);
        return sqlState.startsWith("22") || sqlState.startsWith("23")
                ? new DataIntegrityViolationException("rejected", cause)
                : new DataAccessResourceFailureException("failed", cause);
    }
}
