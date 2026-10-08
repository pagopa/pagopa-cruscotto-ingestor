package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifica l'esclusione degli enti creditori sulle query realmente generate, template per template.
 *
 * <p>Due proprieta' da proteggere, entrambe con conseguenze silenziose:</p>
 * <ul>
 *   <li><strong>Nessun segnaposto deve restare nella query.</strong>
 *       {@code QueryTemplateLoader} sostituisce solo le chiavi che il builder gli passa e lascia
 *       le altre com'erano: un {@code ${pa_filter}} dimenticato finirebbe dentro il KQL e ADX
 *       rifiuterebbe la query. E' un difetto che compare solo a runtime, sull'entita' scoperta.</li>
 *   <li><strong>L'esclusione vale per tutte le entita'.</strong> Le FK sono obbligatorie: filtrare
 *       solo POSITION lascerebbe i token senza padre, che finirebbero in STG_INGEST_ERROR come
 *       MISSING_FOREIGN_KEY insieme a transfer ed extra info. Il risparmio diventerebbe
 *       un accumulo di scarti.</li>
 * </ul>
 */
class AdxQueryTemplatePaExclusionTest {

    private static final Instant FROM = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-10-01T00:05:00Z");
    private static final String PA_TEST = "77777777777";

    private static AdxTableNamesConfig tableNames() {
        AdxTableNamesConfig config = new AdxTableNamesConfig();
        config.setTables(Map.of(
                "POSITION", "SERT_POSITION",
                "POSITION_TOKENS", "SERT_POSITION_TOKENS",
                "POSITION_TRANSFERS", "SERT_TRANSFERS",
                "EXTRA_INFO", "SERT_EXTRA_INFO",
                "EVENTS_WF", "SERT_EVENTS_WF"));
        return config;
    }

    private static IngestionConfigProvider provider(List<String> excluded) {
        IngestionConfig config = new IngestionConfig();
        config.getAdx().setExcludedPaEmittenti(new ArrayList<>(excluded));
        return new IngestionConfigProvider(config);
    }

    /** Le query prodotte da ogni entita', per la lista di esclusione data. */
    private static Map<String, String> allQueries(List<String> excluded) {
        QueryTemplateLoader loader = new QueryTemplateLoader();
        IngestionConfigProvider cfg = provider(excluded);
        AdxTableNamesConfig tables = tableNames();

        Function<EntityName, RunContext> ctx = e -> new RunContext(e.name(), "run-1", Instant.now());
        EventsWfAdxQueryBuilder eventsWf = new EventsWfAdxQueryBuilder(loader, cfg, tables);

        return Map.of(
                "POSITION", new PositionAdxQueryBuilder(loader, cfg, tables)
                        .buildQuery(ctx.apply(EntityName.POSITION), FROM, TO),
                "POSITION_TOKENS", new PositionTokensAdxQueryBuilder(loader, cfg, tables)
                        .buildQuery(ctx.apply(EntityName.POSITION_TOKENS), FROM, TO),
                "POSITION_TRANSFERS", new TransfersAdxQueryBuilder(loader, cfg, tables)
                        .buildQuery(ctx.apply(EntityName.POSITION_TRANSFERS), FROM, TO),
                "EXTRA_INFO", new ExtraInfoAdxQueryBuilder(loader, cfg, tables)
                        .buildQuery(ctx.apply(EntityName.EXTRA_INFO), FROM, TO),
                "EVENTS_WF", eventsWf.buildQuery(ctx.apply(EntityName.EVENTS_WF), FROM, TO));
    }

    @Test
    void nessunaQueryLasciaSegnapostoNonSostituiti() {
        allQueries(List.of(PA_TEST)).forEach((entity, query) ->
                assertFalse(query.contains("${"),
                        "segnaposto non sostituito in " + entity + ": "
                                + query.substring(Math.max(0, query.indexOf("${")))));
    }

    @Test
    void nessunaQueryLasciaSegnapostoNemmenoSenzaEsclusioni() {
        allQueries(List.of()).forEach((entity, query) ->
                assertFalse(query.contains("${"), "segnaposto non sostituito in " + entity));
    }

    @Test
    void ogniEntitaApplicaLEsclusione() {
        allQueries(List.of(PA_TEST)).forEach((entity, query) ->
                assertTrue(query.contains("!in (\"" + PA_TEST + "\")"),
                        "l'entita' " + entity + " non filtra l'ente escluso: i suoi figli finirebbero"
                                + " in staging come MISSING_FOREIGN_KEY"));
    }

    /**
     * EVENTS_WF legge la tabella due volte (gamba REQ e gamba RESP) e POSITION_TOKENS pure (fee e
     * query principale): il filtro va su entrambe, altrimenti una meta' passerebbe comunque.
     */
    @Test
    void leEntitaConDueLettureFiltranoEntrambeLeGambe() {
        Map<String, String> queries = allQueries(List.of(PA_TEST));

        assertEquals(2, countOccurrences(queries.get("EVENTS_WF"), "!in (\"" + PA_TEST + "\")"),
                "EVENTS_WF deve filtrare sia la gamba REQ sia la RESP");
        assertEquals(2, countOccurrences(queries.get("POSITION_TOKENS"), "!in (\"" + PA_TEST + "\")"),
                "POSITION_TOKENS deve filtrare sia la lettura delle fee sia quella principale");
    }

    /** Default a lista vuota: nessuna query deve contenere un filtro sugli enti. */
    @Test
    void senzaConfigurazioneNessunaQueryFiltraPerEnte() {
        allQueries(List.of()).forEach((entity, query) ->
                assertFalse(query.contains("tostring(PA_EMITTENTE)) !in"),
                        "filtro applicato senza configurarlo in " + entity));
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
}
