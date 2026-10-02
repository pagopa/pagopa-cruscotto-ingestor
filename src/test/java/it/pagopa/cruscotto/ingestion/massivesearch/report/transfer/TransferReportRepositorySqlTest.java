package it.pagopa.cruscotto.ingestion.massivesearch.report.transfer;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the temporal semantics of the transfer report SQL: righe limitate alla finestra,
 * {@code TOKEN_COUNT} overall, {@code TRANSFER_NUMBER} relativo al singolo token.
 */
class TransferReportRepositorySqlTest {

    private static final AnalysisWindow WINDOW =
        new AnalysisWindow(LocalDateTime.parse("2026-03-01T00:00:00"), LocalDateTime.parse("2026-04-01T00:00:00"));

    private final TransferReportRepository repository = new TransferReportRepository(null, schemaConfig());

    private static DbSchemaConfig schemaConfig() {
        DbSchemaConfig config = new DbSchemaConfig();
        config.setSchema("ingestor");
        return config;
    }

    @Test
    void tokenCountIsOverallWhileTheReportRowsAreWindowed() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);

        assertTrue(sql.contains("JOIN ingestor.position_tokens t ON t.fk_position = p.id"
            + " AND t.inserted_timestamp >= CAST(:winFrom AS timestamp)"), sql);
        String countLateral = countLateral(sql);
        assertFalse(countLateral.contains("inserted_timestamp"), countLateral);
        assertFalse(countLateral.contains(":winFrom"), countLateral);
    }

    @Test
    void transfersFollowTheirTokenAndAreNotWindowedOnTheirOwn() {
        // I transfer non hanno una finestra propria: dipendono dal token, gia' filtrato.
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertTrue(sql.contains("JOIN ingestor.position_transfers tr ON tr.fk_token = t.id"), sql);
        assertFalse(sql.contains("tr.inserted_timestamp"), sql);
    }

    @Test
    void transferNumberStaysScopedToTheSingleToken() {
        assertTrue(repository.buildBaseSelect("ingestor", WINDOW).contains("tr2.fk_token = t.id"));
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

    private static String countLateral(String sql) {
        int start = sql.indexOf("COUNT(*) AS token_count");
        assertTrue(start > 0, "token_count LATERAL non trovata");
        int end = sql.indexOf(") tkagg ON TRUE", start);
        assertTrue(end > start, "delimitatore della LATERAL non trovato");
        return sql.substring(start, end);
    }
}
