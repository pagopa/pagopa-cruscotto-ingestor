package it.pagopa.cruscotto.ingestion.massivesearch.report.token;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportWindowSql;
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

    /** Forma reale del join sulle chiavi: deve comparire prima di ogni altro join. */
    private static final String KEY_JOIN =
        "JOIN (VALUES (CAST(:kj_0 AS text))) AS k(nav) ON p.nav = k.nav";

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
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);
        assertTrue(sql.contains("tr.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("tr.date_event <= " + ReportWindowSql.positionLastDate("p") + " + CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("ei.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("ei.date_event <= " + ReportWindowSql.positionLastDate("p") + " + CAST(:childMarginDays AS integer)"), sql);
    }

    @Test
    void childPruningIsIndependentOfTheAnalysisWindow() {
        // Il bound deriva dal token padre, non dalla finestra utente: deve valere anche senza periodo.
        String sql = repository.buildBaseSelect("ingestor", AnalysisWindow.none(), KEY_JOIN);
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
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);
        assertFalse(sql.contains("- CAST(:childMarginDays"), sql);
    }

    /**
     * In questo report la finestra insiste sul token, quindi i figli non possono cadere fuori da essa:
     * oltre al bound correlato (pruning a runtime) ne viene emesso uno costante, che fa potare le
     * partizioni gia' in planning.
     */
    @Test
    void childrenAlsoGetAConstantBoundDerivedFromTheWindow() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);
        assertTrue(sql.contains("tr.date_event >= CAST(:winFrom AS date)"), sql);
        assertTrue(sql.contains("ei.date_event >= CAST(:winFrom AS date)"), sql);
    }

    @Test
    void childPruningCanBeDisabledFromConfiguration() {
        MassiveSearchProperties disabled = new MassiveSearchProperties();
        disabled.getExecution().setChildDateMarginDays(0);
        TokenReportRepository repo = new TokenReportRepository(null, schemaConfig(), disabled);

        assertFalse(repo.buildBaseSelect("ingestor", WINDOW, KEY_JOIN).contains(":childMarginDays"));
    }

    @Test
    void tokenCountIsOverallWhileTheReportRowsAreWindowed() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);

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
        String countLateral = countLateral(repository.buildBaseSelect("ingestor", AnalysisWindow.none(), KEY_JOIN));
        assertTrue(countLateral.contains("COUNT(*) AS token_count"), countLateral);
        assertFalse(countLateral.contains("inserted_timestamp"), countLateral);
    }

    @Test
    void transferCountStaysScopedToTheSingleToken() {
        // Spec: "transfer_count e' relativo ad un singolo TOKEN", mai alla posizione.
        assertTrue(repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN).contains("tr.fk_token = t.id"));
    }

    @Test
    void theWindowPrunesPartitionsOnTheTokenJoin() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);
        assertTrue(sql.contains("t.date_event >= CAST(:winFrom AS date)"), sql);
        assertTrue(sql.contains("t.date_event <= CAST(:winTo AS date)"), sql);
    }

    @Test
    void unboundedWindowEmitsNoWindowParameters() {
        String sql = repository.buildBaseSelect("ingestor", AnalysisWindow.none(), KEY_JOIN);
        assertFalse(sql.contains(":winFrom"), sql);
        assertFalse(sql.contains(":winTo"), sql);
    }

    /** Estrae la sola LATERAL che calcola token_count. */
    /**
     * Il join sulle chiavi deve stare <strong>subito dopo</strong> {@code FROM position p}, prima di
     * ogni altro join.
     *
     * <p>La FROM conta 12 relazioni contro un {@code join_collapse_limit} di 8: oltre quel limite
     * PostgreSQL non riordina piu' i join ed esegue l'ordine scritto. Con le chiavi concatenate in
     * coda — come era prima — il piano partiva da {@code p JOIN t} senza alcun filtro, cioe' tutti i
     * token del database espansi attraverso tre LATERAL, e restringeva per chiave solo alla fine:
     * misurato in collaudo, 5 minuti interrotti dal timeout per produrre 2 righe.</p>
     */
    @Test
    void theKeyJoinMustDriveThePlanAndComeBeforeEveryOtherJoin() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);

        int keyJoinAt = sql.indexOf(KEY_JOIN);
        assertTrue(keyJoinAt > 0, "join sulle chiavi assente: " + sql);

        int fromPositionAt = sql.indexOf("FROM ingestor.position p");
        assertTrue(fromPositionAt > 0 && fromPositionAt < keyJoinAt,
            "il join sulle chiavi deve seguire FROM position p");

        // Nessun altro join puo' precederlo: sono quelli che, eseguiti prima, aprono l'intera tabella.
        assertTrue(sql.indexOf("JOIN ingestor.position_tokens t") > keyJoinAt,
            "il join su position_tokens precede le chiavi: il piano partirebbe senza filtro");
        assertTrue(sql.indexOf("LEFT JOIN LATERAL") > keyJoinAt,
            "una LATERAL precede le chiavi");
    }

    private static String countLateral(String sql) {
        int start = sql.indexOf("COUNT(*) AS token_count");
        assertTrue(start > 0, "token_count LATERAL non trovata");
        int end = sql.indexOf(") agg ON TRUE", start);
        assertEquals(true, end > start, "delimitatore della LATERAL non trovato");
        return sql.substring(start, end);
    }
}
