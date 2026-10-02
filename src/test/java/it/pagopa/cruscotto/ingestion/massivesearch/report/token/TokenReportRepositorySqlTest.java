package it.pagopa.cruscotto.ingestion.massivesearch.report.token;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the temporal semantics of the token report SQL, che la specifica cliente fissa cosi':
 * le righe del report sono limitate alla finestra, mentre {@code TOKEN_COUNT} e' "overall" (tutti i
 * tentativi della posizione presenti a sistema). Sono due regole opposte sulla stessa tabella,
 * quindi facilissime da "uniformare" per errore.
 */
class TokenReportRepositorySqlTest {

    private static final AnalysisWindow WINDOW =
        new AnalysisWindow(LocalDateTime.parse("2026-03-01T00:00:00"), LocalDateTime.parse("2026-04-01T00:00:00"));

    private final TokenReportRepository repository = new TokenReportRepository(null, schemaConfig());

    private static DbSchemaConfig schemaConfig() {
        DbSchemaConfig config = new DbSchemaConfig();
        config.setSchema("ingestor");
        return config;
    }

    @Test
    void tokenCountIsOverallWhileTheReportRowsAreWindowed() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);

        // La join che produce le righe deve essere filtrata...
        assertTrue(sql.contains("JOIN ingestor.position_tokens t ON t.fk_position = p.id"
            + " AND t.inserted_timestamp >= CAST(:winFrom AS timestamp)"), sql);
        // ...mentre la LATERAL del conteggio no.
        String countLateral = countLateral(sql);
        assertFalse(countLateral.contains("inserted_timestamp"), countLateral);
        assertFalse(countLateral.contains(":winFrom"), countLateral);
        assertFalse(countLateral.contains(":winTo"), countLateral);
    }

    @Test
    void tokenCountStaysUnfilteredEvenWithoutAWindow() {
        String countLateral = countLateral(repository.buildBaseSelect("ingestor", AnalysisWindow.none()));
        assertTrue(countLateral.contains("COUNT(*) AS token_count"), countLateral);
        assertFalse(countLateral.contains("inserted_timestamp"), countLateral);
    }

    @Test
    void transferCountStaysScopedToTheSingleToken() {
        // Spec: "transfer_count e' relativo ad un singolo TOKEN", mai alla posizione.
        assertTrue(repository.buildBaseSelect("ingestor", WINDOW).contains("tr.fk_token = t.id"));
    }

    @Test
    void theWindowNeverTouchesThePartitionKey() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertFalse(sql.contains("t.date_event >="), sql);
        assertFalse(sql.contains("t.date_event <"), sql);
    }

    @Test
    void unboundedWindowEmitsNoWindowParameters() {
        String sql = repository.buildBaseSelect("ingestor", AnalysisWindow.none());
        assertFalse(sql.contains(":winFrom"), sql);
        assertFalse(sql.contains(":winTo"), sql);
    }

    /** Estrae la sola LATERAL che calcola token_count. */
    private static String countLateral(String sql) {
        int start = sql.indexOf("COUNT(*) AS token_count");
        assertTrue(start > 0, "token_count LATERAL non trovata");
        int end = sql.indexOf(") agg ON TRUE", start);
        assertEquals(true, end > start, "delimitatore della LATERAL non trovato");
        return sql.substring(start, end);
    }
}
