package it.pagopa.cruscotto.ingestion.massivesearch.report.position;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the temporal semantics of the position report SQL. Qui convivono due aggregati sulla stessa
 * tabella con regole opposte: {@code TOKEN_COUNT} e' overall (tutti i tentativi a sistema) mentre
 * {@code DATE_PAYED}/{@code IS_PAYED} restano relativi alla finestra. Erano un'unica LATERAL: se
 * qualcuno le riunisce, una delle due semantiche si rompe silenziosamente.
 */
class PositionReportRepositorySqlTest {

    private static final AnalysisWindow WINDOW =
        new AnalysisWindow(LocalDateTime.parse("2026-03-01T00:00:00"), LocalDateTime.parse("2026-04-01T00:00:00"));

    private final PositionReportRepository repository = new PositionReportRepository(null, schemaConfig());

    private static DbSchemaConfig schemaConfig() {
        DbSchemaConfig config = new DbSchemaConfig();
        config.setSchema("ingestor");
        return config;
    }

    @Test
    void tokenCountIsOverallWhilePaymentAggregatesStayWindowed() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);

        String countLateral = between(sql, "COUNT(*) AS token_count", ") tkall ON TRUE");
        assertFalse(countLateral.contains("inserted_timestamp"), countLateral);

        String paymentLateral = between(sql, "MIN(tks.payment_date)", ") agg ON TRUE");
        assertTrue(paymentLateral.contains("tks.inserted_timestamp >= CAST(:winFrom AS timestamp)"), paymentLateral);
    }

    @Test
    void datePayedIsNotFilteredByOutcomeSoItMatchesTheOtherTwoReports() {
        // Allineato a Token/Transfer: payment_date e' scritta dalla prima SPO con OUTCOME_REQ='OK'
        // anche quando OUTCOME_RESP='KO' (pagamento avvenuto, esito non consolidato): la data va
        // comunque esposta. IS_PAYED resta invece legato a outcome='OK'.
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertTrue(sql.contains("MIN(tks.payment_date) AS date_payed"), sql);
        assertFalse(sql.contains("MIN(tks.payment_date) FILTER"), sql);
        assertTrue(sql.contains("BOOL_OR(tks.outcome = 'OK') AS is_payed"), sql);
    }

    @Test
    void theRepresentativeTokenIsPickedWithinTheWindow() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertTrue(sql.contains("WHERE tk.fk_position = p.id"
            + " AND tk.inserted_timestamp >= CAST(:winFrom AS timestamp)"), sql);
    }

    @Test
    void theWindowNeverTouchesThePartitionKey() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertFalse(sql.contains("tk.date_event >="), sql);
        assertFalse(sql.contains("tks.date_event <"), sql);
    }

    @Test
    void unboundedWindowEmitsNoWindowParameters() {
        String sql = repository.buildBaseSelect("ingestor", AnalysisWindow.none());
        assertFalse(sql.contains(":winFrom"), sql);
        assertFalse(sql.contains(":winTo"), sql);
    }

    private static String between(String sql, String from, String to) {
        int start = sql.indexOf(from);
        assertTrue(start > 0, "frammento non trovato: " + from);
        int end = sql.indexOf(to, start);
        assertTrue(end > start, "delimitatore non trovato: " + to);
        return sql.substring(start, end);
    }
}
