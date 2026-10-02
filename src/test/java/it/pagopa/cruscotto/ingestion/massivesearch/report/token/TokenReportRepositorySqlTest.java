package it.pagopa.cruscotto.ingestion.massivesearch.report.token;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
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

    private final TokenReportRepository repository = new TokenReportRepository(null, schemaConfig(), properties());

    private static DbSchemaConfig schemaConfig() {
        DbSchemaConfig config = new DbSchemaConfig();
        config.setSchema("ingestor");
        return config;
    }

    private static MassiveSearchProperties properties() {
        MassiveSearchProperties props = new MassiveSearchProperties();
        props.getExecution().setChildDateMarginDays(15);
        return props;
    }

    @Test
    void childLateralsPruneOnTheTokenDateEvent() {
        // trf e xi sono correlate solo su fk_token e non hanno finestra: senza questo bound ogni
        // lookup apre tutte le ~25 partizioni mensili per leggere 1-2 righe.
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertTrue(sql.contains("tr.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("tr.date_event <= t.date_event + CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("ei.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("ei.date_event <= t.date_event + CAST(:childMarginDays AS integer)"), sql);
    }

    @Test
    void childPruningIsIndependentOfTheAnalysisWindow() {
        // Il bound deriva dal token padre, non dalla finestra utente: deve valere anche senza periodo.
        String sql = repository.buildBaseSelect("ingestor", AnalysisWindow.none());
        assertTrue(sql.contains("tr.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("ei.date_event >= t.date_event AND"), sql);
    }

    /**
     * Un figlio non puo' precedere il proprio padre, quindi il margine vale solo in avanti. Il bound
     * simmetrico raddoppiava l'ampiezza della finestra senza coprire alcun caso reale: questa guardia
     * impedisce che torni.
     */
    @Test
    void theChildBoundIsAsymmetric() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertFalse(sql.contains("- CAST(:childMarginDays"), sql);
    }

    /**
     * In questo report la finestra insiste sul token, quindi i figli non possono cadere fuori da essa:
     * oltre al bound correlato (pruning a runtime) ne viene emesso uno costante, che fa potare le
     * partizioni gia' in planning.
     */
    @Test
    void childrenAlsoGetAConstantBoundDerivedFromTheWindow() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertTrue(sql.contains("tr.date_event >= CAST(:winFrom AS date)"), sql);
        assertTrue(sql.contains("tr.date_event <= CAST(:winTo AS date) + CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("ei.date_event >= CAST(:winFrom AS date)"), sql);
        assertTrue(sql.contains("ei.date_event <= CAST(:winTo AS date) + CAST(:childMarginDays AS integer)"), sql);
    }

    @Test
    void childPruningCanBeDisabledFromConfiguration() {
        MassiveSearchProperties disabled = new MassiveSearchProperties();
        disabled.getExecution().setChildDateMarginDays(0);
        TokenReportRepository repo = new TokenReportRepository(null, schemaConfig(), disabled);

        assertFalse(repo.buildBaseSelect("ingestor", WINDOW).contains(":childMarginDays"));
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
    void theWindowPrunesPartitionsOnTheTokenJoin() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW);
        assertTrue(sql.contains("t.date_event >= CAST(:winFrom AS date)"), sql);
        assertTrue(sql.contains("t.date_event <= CAST(:winTo AS date)"), sql);
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
