package it.pagopa.cruscotto.ingestion.massivesearch.report.position;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportKeyJoinSql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Blocca la semantica temporale del report Position, che e' <strong>opposta</strong> a quella dei
 * report Tentativi e Transfer.
 *
 * <p>Scenario di riferimento del cliente: la stessa (NAV, EC) presente a gennaio senza tentativi e a
 * marzo con un tentativo. Cercando gennaio il report deve produrre <em>una</em> riga, con
 * {@code TOKEN_COUNT = 1} e i dati del tentativo di marzo. Da qui le tre regole verificate qui
 * sotto: la finestra seleziona le <em>posizioni</em> e non i tentativi, i tentativi si raccolgono su
 * <em>tutte</em> le occorrenze della business key, e una posizione senza tentativi non sparisce.</p>
 */
class PositionReportRepositorySqlTest {

    private static final AnalysisWindow WINDOW =
        new AnalysisWindow(LocalDateTime.parse("2026-03-01T00:00:00"), LocalDateTime.parse("2026-04-01T00:00:00"));

    private static final String KEY_JOIN = "JOIN (VALUES (CAST(:kj_0 AS text))) AS k(nav) ON p.nav = k.nav";

    private final PositionReportRepository repository = new PositionReportRepository(null, schemaConfig(), properties());

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

    private String sql(AnalysisWindow window) {
        return repository.buildBaseSelect("ingestor", window, KEY_JOIN);
    }

    @Test
    void theWindowSelectsPositionsAndNotTokens() {
        // Requisito: una posizione presente nel periodo va riportata anche se il suo unico tentativo
        // cade fuori. Se la finestra tornasse sulle LATERAL dei token, lo scenario del cliente
        // (posizione di gennaio, tentativo di marzo) produrrebbe zero righe.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("p.inserted_timestamp >= CAST(:winFrom AS timestamp)"), sql);
        assertTrue(sql.contains("p.inserted_timestamp < CAST(:winTo AS timestamp)"), sql);
        assertFalse(sql.contains("tk.inserted_timestamp >= CAST(:winFrom AS timestamp)"), sql);
        assertFalse(sql.contains("tks.inserted_timestamp >= CAST(:winFrom AS timestamp)"), sql);
    }

    @Test
    void theWindowPrunesPartitionsOnPosition() {
        // date_event accanto a inserted_timestamp: il primo porta la precisione al secondo richiesta
        // dall'utente, il secondo e' implicato e serve solo al planner per potare le partizioni.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("p.date_event >= CAST(:winFrom AS date)"), sql);
        // '<=' e non '<': winTo e' esclusivo sul timestamp ma inclusivo sulla data troncata.
        assertTrue(sql.contains("p.date_event <= CAST(:winTo AS date)"), sql);
    }

    @Test
    void tokenLateralsSpanEveryOccurrenceOfTheBusinessKey() {
        // TOKEN_COUNT = 1 nello scenario del cliente conta un tentativo appeso all'occorrenza di
        // marzo mentre la ricerca e' di gennaio: le LATERAL non possono correlare su p.id.
        String sql = sql(WINDOW);

        assertFalse(sql.contains("tk.fk_position = p.id"), sql);
        assertFalse(sql.contains("tks.fk_position = p.id"), sql);
        assertTrue(sql.contains("p2.nav = pk.nav AND p2.pa_emittente = pk.pa"), sql);
    }

    @Test
    void positionsWithoutAnyTokenStillProduceARow() {
        // LEFT e non INNER: la posizione di gennaio non ha tentativi e deve comparire ugualmente,
        // con i campi del token vuoti.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("LEFT JOIN LATERAL ("), sql);
        assertFalse(sql.contains(" JOIN LATERAL ( SELECT tk.*"), sql);
        assertFalse(sql.contains("FROM ingestor.position p JOIN LATERAL"), sql);
    }

    @Test
    void oneRowPerBusinessKeyEvenWithSeveralPositionOccurrences() {
        // La stessa (NAV, EC) puo' avere piu' occorrenze in POSITION: il report ne emette una sola.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("SELECT DISTINCT p.nav AS nav, p.pa_emittente AS pa"), sql);
        assertTrue(sql.contains(KEY_JOIN), sql);
        assertTrue(sql.contains(") pk"), sql);
        assertTrue(sql.contains("pk.nav AS nav"), sql);
    }

    @Test
    void tokenCountIsOverallAndNeverWindowed() {
        String sql = sql(WINDOW);
        String countLateral = between(sql, "SELECT MIN(tks.payment_date) AS date_payed", ") agg ON TRUE");

        assertTrue(countLateral.contains("COUNT(*) AS token_count"), countLateral);
        assertFalse(countLateral.contains(":winFrom"), countLateral);
        assertFalse(countLateral.contains(":winTo"), countLateral);
    }

    @Test
    void tokensArePrunedAgainstTheirOwnPositionOccurrence() {
        // Persa la finestra sui token, il pruning si recupera correlando il token alla propria
        // occorrenza di posizione: l'ingestion le associa entro 24h, quindi il margine e' larghissimo.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("tk.date_event >= p2.date_event - CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("tk.date_event <= p2.date_event + CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("tks.date_event >= p2.date_event - CAST(:childMarginDays AS integer)"), sql);
    }

    @Test
    void childLateralsPruneOnTheTokenDateEvent() {
        // trf e xi sono correlate solo su fk_token e non hanno finestra: senza questo bound ogni
        // lookup aprirebbe tutte le partizioni mensili.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("tr.date_event >= t.date_event - CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("tr.date_event <= t.date_event + CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("ei.date_event >= t.date_event - CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("ei.date_event <= t.date_event + CAST(:childMarginDays AS integer)"), sql);
    }

    @Test
    void childPruningIsIndependentOfTheAnalysisWindow() {
        String sql = sql(AnalysisWindow.none());

        assertTrue(sql.contains("tr.date_event >= t.date_event - CAST(:childMarginDays AS integer)"), sql);
        assertTrue(sql.contains("tk.date_event >= p2.date_event - CAST(:childMarginDays AS integer)"), sql);
    }

    @Test
    void childPruningCanBeDisabledFromConfiguration() {
        MassiveSearchProperties props = new MassiveSearchProperties();
        props.getExecution().setChildDateMarginDays(0);
        PositionReportRepository repo = new PositionReportRepository(null, schemaConfig(), props);

        assertFalse(repo.buildBaseSelect("ingestor", WINDOW, KEY_JOIN).contains(":childMarginDays"));
    }

    @Test
    void theRepresentativeTokenIsTheCashedOneOtherwiseTheLatest() {
        // Spec DATE_BORN: attivazione del token incassato, altrimenti dell'ultimo disponibile.
        // I token non incassati hanno payment_date nulla, quindi a ordinarli e' inserted_timestamp.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("ORDER BY (CASE WHEN tk.outcome = 'OK' THEN 0 ELSE 1 END)"), sql);
        assertTrue(sql.contains("tk.payment_date DESC NULLS LAST, tk.inserted_timestamp DESC NULLS LAST, tk.id DESC"), sql);
        assertTrue(sql.contains("LIMIT 1"), sql);
    }

    @Test
    void datePayedIsNotFilteredByOutcomeSoItMatchesTheOtherTwoReports() {
        // DATE_PAYED e' la prima SPO pervenuta: un FILTER su outcome la renderebbe incoerente con i
        // report Token e Transfer, che non lo applicano.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("MIN(tks.payment_date) AS date_payed"), sql);
        assertFalse(sql.contains("MIN(tks.payment_date) FILTER"), sql);
        assertTrue(sql.contains("BOOL_OR(tks.outcome = 'OK') AS is_payed"), sql);
    }

    @Test
    void unboundedWindowEmitsNoWindowParameters() {
        String sql = sql(AnalysisWindow.none());

        assertFalse(sql.contains(":winFrom"), sql);
        assertFalse(sql.contains(":winTo"), sql);
    }

    @Test
    void theThreeAggregatesShareASingleScan() {
        // agg, token_count e is_payed hanno la stessa FROM/WHERE: tenerli su LATERAL separate
        // raddoppiava le probe su position, che per requisito non possono essere potate per periodo.
        String sql = sql(WINDOW);

        assertTrue(sql.contains("COUNT(*) AS token_count"), sql);
        assertTrue(sql.contains("agg.token_count AS token_count"), sql);
        assertFalse(sql.contains(") tkall ON TRUE"), sql);
        assertEquals(1, countOccurrences(sql, "JOIN " + "ingestor.position p2 ON p2.id = tks.fk_position"), sql);
    }

    @ParameterizedTest
    @EnumSource(value = CsvTemplate.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
    void everyTemplateProducesAStructurallyValidStatement(CsvTemplate template) {
        // Il key join e' iniettato dentro la subquery che riduce alle business key distinte: IUV,
        // IUV_PA e TOKEN iniettano a loro volta una subquery, ed e' la combinazione che questo
        // changeset ha introdotto. Un test sul solo frammento NAV non la coprirebbe.
        MapSqlParameterSource params = new MapSqlParameterSource();
        String keyJoin = ReportKeyJoinSql.buildKeyJoin(template, "ingestor",
            List.of(new SearchInputRow("302001", "77777777777", "001", "tok-1")), params);
        assertNotNull(keyJoin, "nessun key join per il template " + template);

        String sql = repository.buildBaseSelect("ingestor", WINDOW, keyJoin);

        assertEquals(countOccurrences(sql, "("), countOccurrences(sql, ")"), "parentesi sbilanciate: " + sql);
        // Il frammento deve finire dentro la subquery pk, prima che le LATERAL la correlino.
        int keyJoinAt = sql.indexOf(keyJoin);
        assertTrue(keyJoinAt > 0, sql);
        assertTrue(keyJoinAt < sql.indexOf(") pk"), "key join fuori dalla subquery delle chiavi: " + sql);
        assertTrue(sql.contains("SELECT DISTINCT p.nav AS nav, p.pa_emittente AS pa"), sql);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int at = haystack.indexOf(needle);
        while (at >= 0) {
            count++;
            at = haystack.indexOf(needle, at + needle.length());
        }
        return count;
    }

    private static String between(String sql, String from, String to) {
        int start = sql.indexOf(from);
        assertTrue(start > 0, "frammento non trovato: " + from);
        int end = sql.indexOf(to, start);
        assertTrue(end > start, "delimitatore non trovato: " + to);
        return sql.substring(start, end);
    }
}
