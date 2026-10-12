package it.pagopa.cruscotto.ingestion.massivesearch.report.transfer;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportKeyJoinSql;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportWindowSql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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

    /** Forma reale del join sulle chiavi: deve comparire prima di ogni altro join. */
    private static final String KEY_JOIN =
        "JOIN (VALUES (CAST(:kj_0 AS text))) AS k(nav) ON p.nav = k.nav";

    private final TransferReportRepository repository = new TransferReportRepository(null, schemaConfig(), properties());

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
    void childJoinAndLateralsPruneOnTheTokenDateEvent() {
        // Qui il transfer e' anche la riga del report (JOIN principale), non solo una LATERAL:
        // senza bound il JOIN apre tutte le ~25 partizioni mensili per ogni token.
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);
        assertTrue(sql.contains("tr.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("tr2.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("ei.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("tr.date_event <= " + ReportWindowSql.positionLastDate("p") + " + CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("tr2.date_event <= " + ReportWindowSql.positionLastDate("p") + " + CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("ei.date_event <= " + ReportWindowSql.positionLastDate("p") + " + CAST(:childMarginDays AS integer)"), sql);
    }

    @Test
    void childPruningIsIndependentOfTheAnalysisWindow() {
        // Il bound deriva dal token padre, non dalla finestra utente: deve valere anche senza periodo.
        String sql = repository.buildBaseSelect("ingestor", AnalysisWindow.none(), KEY_JOIN);
        assertTrue(sql.contains("tr.date_event >= t.date_event AND"), sql);
        assertTrue(sql.contains("ei.date_event >= t.date_event AND"), sql);
    }

    @Test
    void childPruningCanBeDisabledFromConfiguration() {
        MassiveSearchProperties disabled = new MassiveSearchProperties();
        disabled.getExecution().setChildDateMarginDays(0);
        TransferReportRepository repo = new TransferReportRepository(null, schemaConfig(), disabled);

        assertFalse(repo.buildBaseSelect("ingestor", WINDOW, KEY_JOIN).contains(":childMarginDays"));
    }

    /** Il margine vale solo in avanti: un figlio non puo' precedere il proprio padre. */
    @Test
    void theChildBoundIsAsymmetric() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);
        assertFalse(sql.contains("- CAST(:childMarginDays"), sql);
    }

    /**
     * La finestra insiste sul token, quindi i figli ereditano anche un bound costante: fa potare le
     * partizioni in planning, non solo a runtime.
     */
    @Test
    void childrenAlsoGetAConstantBoundDerivedFromTheWindow() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);
        assertTrue(sql.contains("tr.date_event >= CAST(:winFrom AS date)"), sql);
        assertTrue(sql.contains("tr2.date_event >= CAST(:winFrom AS date)"), sql);
        assertTrue(sql.contains("ei.date_event >= CAST(:winFrom AS date)"), sql);
    }

    @Test
    void tokenCountIsOverallWhileTheReportRowsAreWindowed() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);

        assertTrue(sql.contains("JOIN ingestor.position_tokens t ON t.fk_position = p.id"
            + " AND t.inserted_timestamp >= CAST(:winFrom AS timestamp)"), sql);
        String countLateral = countLateral(sql);
        assertFalse(countLateral.contains("inserted_timestamp"), countLateral);
        assertFalse(countLateral.contains(":winFrom"), countLateral);
    }

    @Test
    void transfersFollowTheirTokenAndAreNotWindowedOnTheirOwn() {
        // I transfer non hanno una finestra propria: dipendono dal token, gia' filtrato.
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);
        assertTrue(sql.contains("JOIN ingestor.position_transfers tr ON tr.fk_token = t.id"), sql);
        assertFalse(sql.contains("tr.inserted_timestamp"), sql);
    }

    @Test
    void transferNumberStaysScopedToTheSingleToken() {
        assertTrue(repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN).contains("tr2.fk_token = t.id"));
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

    /**
     * Come per il report dei tentativi: il join sulle chiavi deve guidare il piano, quindi stare
     * subito dopo {@code FROM position p}. Oltre {@code join_collapse_limit} PostgreSQL esegue i join
     * nell'ordine scritto, e con le chiavi in coda si parte dall'intera tabella dei token.
     */
    @Test
    void theKeyJoinMustDriveThePlanAndComeBeforeEveryOtherJoin() {
        String sql = repository.buildBaseSelect("ingestor", WINDOW, KEY_JOIN);

        int keyJoinAt = sql.indexOf(KEY_JOIN);
        assertTrue(keyJoinAt > 0, "join sulle chiavi assente: " + sql);

        int fromPositionAt = sql.indexOf("FROM ingestor.position p");
        assertTrue(fromPositionAt > 0 && fromPositionAt < keyJoinAt,
            "il join sulle chiavi deve seguire FROM position p");

        assertTrue(sql.indexOf("JOIN ingestor.position_tokens t") > keyJoinAt,
            "il join su position_tokens precede le chiavi: il piano partirebbe senza filtro");
        assertTrue(sql.indexOf("JOIN ingestor.position_transfers tr") > keyJoinAt,
            "il join su position_transfers precede le chiavi");
    }

    /** Come per il report Tentativi: la garanzia vale su tutti e cinque i template, non solo su NAV. */
    @ParameterizedTest
    @EnumSource(value = CsvTemplate.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
    void everyTemplateKeyJoinDrivesThePlanAndReferencesOnlyPosition(CsvTemplate template) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String keyJoin = ReportKeyJoinSql.buildKeyJoin(template, "ingestor",
            List.of(new SearchInputRow("302001", "77777777777", "96000020024529651", "tok-1")), params);
        assertNotNull(keyJoin, "nessun join costruito per il template " + template);

        String sql = repository.buildBaseSelect("ingestor", WINDOW, keyJoin);

        int keyJoinAt = sql.indexOf(keyJoin);
        assertTrue(keyJoinAt > sql.indexOf("FROM ingestor.position p"),
            "il join sulle chiavi deve seguire FROM position p: " + template);
        assertTrue(sql.indexOf("JOIN ingestor.position_tokens t ON") > keyJoinAt,
            "position_tokens precede le chiavi: " + template);
        assertTrue(sql.indexOf("JOIN ingestor.position_transfers tr") > keyJoinAt,
            "position_transfers precede le chiavi: " + template);

        String outerOn = keyJoin.substring(keyJoin.lastIndexOf(" ON "));
        for (String laterAlias : new String[]{"t.", "tr.", "tr2.", "ei.", "tks."}) {
            assertFalse(outerOn.contains(laterAlias),
                "la ON esterna usa l'alias " + laterAlias + " introdotto dopo (" + template + "): " + outerOn);
        }
    }

    private static String countLateral(String sql) {
        int start = sql.indexOf("COUNT(*) AS token_count");
        assertTrue(start > 0, "token_count LATERAL non trovata");
        int end = sql.indexOf(") tkagg ON TRUE", start);
        assertTrue(end > start, "delimitatore della LATERAL non trovato");
        return sql.substring(start, end);
    }
}
