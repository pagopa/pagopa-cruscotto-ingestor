package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Invarianti strutturali delle due query toccate dalla correzione dei carrelli (multi-payment).
 *
 * <p><strong>Perche' un test sul testo della query.</strong> I template {@code .kql} non sono
 * coperti da nulla: girano solo contro ADX, cioe' solo in ambiente. I due difetti corretti qui
 * erano entrambi <em>silenziosi</em> — nessuna eccezione, nessuna riga in staging, solo dati
 * sbagliati esposti all'utente — e si reintrodurrebbero con una modifica di una riga sola. Finche'
 * non esiste un test di integrazione contro ADX, ancorare la forma della query e' l'unico modo per
 * accorgersene in CI invece che in produzione.</p>
 *
 * <p>Le asserzioni girano sulla query <em>realmente generata</em> dal builder, non sul file sorgente,
 * e solo sulla parte ESEGUIBILE: i commenti vengono rimossi prima del confronto, altrimenti un test
 * come "non deve esserci un inner join" passerebbe o fallirebbe a seconda di come e' scritta la
 * prosa che spiega perche' l'inner join e' stato tolto.</p>
 *
 * @see <a href="file:../../../../../../../.github/instructions/ingestion-handover.instructions.md">§8 dell'handover di ingestion</a>
 */
class AdxQueryTemplateMultiPaymentTest {

    private static final Instant FROM = Instant.parse("2026-03-23T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-03-23T00:06:00Z");

    private static AdxTableNamesConfig tableNames() {
        AdxTableNamesConfig config = new AdxTableNamesConfig();
        config.setTables(Map.of(
                "POSITION_TOKENS", "SERT_POSITION_TOKENS",
                "EVENTS_WF", "SERT_EVENTS_WF"));
        return config;
    }

    private static IngestionConfigProvider provider() {
        return new IngestionConfigProvider(new IngestionConfig());
    }

    /** La query degli eventi, accoppiamento REQ/RESP, senza commenti. */
    private static String eventsWfReqResp() {
        return executablePart(new EventsWfAdxQueryBuilder(new QueryTemplateLoader(), provider(), tableNames())
                .buildReqRespQuery(new RunContext(EntityName.EVENTS_WF.name(), "run-1", Instant.now()), FROM, TO));
    }

    /** La query dei token, con la lookup della commissione, senza commenti. */
    private static String positionTokens() {
        return executablePart(new PositionTokensAdxQueryBuilder(new QueryTemplateLoader(), provider(), tableNames())
                .buildQuery(new RunContext(EntityName.POSITION_TOKENS.name(), "run-1", Instant.now()), FROM, TO));
    }

    /** Toglie le righe di commento KQL, cosi' le asserzioni non leggono la prosa che le spiega. */
    private static String executablePart(String query) {
        return Arrays.stream(query.split("\\R"))
                .filter(line -> !line.trim().startsWith("//"))
                .collect(Collectors.joining("\n"));
    }

    private static int occorrenzeDi(String haystack, String needle) {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length())) {
            count++;
        }
        return count;
    }

    // ----------------------------------------------------------------------------------
    // EVENTS_WF — lo split del carrello deve sopravvivere all'accoppiamento REQ/RESP
    // ----------------------------------------------------------------------------------

    /**
     * Il difetto originale. L'accoppiamento posizionale numerava le righe DENTRO il gruppo
     * (SESSION_ID, TIPO_EVENTO) e le univa in inner join: con N richieste e una sola risposta
     * l'output era min(N, 1) = 1, quindi di un carrello da 5 passava una riga sola e 4 posizioni
     * realmente incassate restavano senza esito.
     */
    @Test
    void lAccoppiamentoNonTornaPosizionale() {
        String query = eventsWfReqResp();

        assertFalse(query.contains("PAIR_SEQ"),
                "PAIR_SEQ accoppia la k-esima REQ con la k-esima RESP: con N REQ e 1 RESP ne sopravvive una sola");
        assertFalse(query.contains("row_number"),
                "la numerazione per riga sfasa anche gli eventi NON carrello che seguono nella stessa sessione");
    }

    /**
     * Il join deve restare leftouter per due ragioni distinte, e nessuna delle due e' estetica:
     * con l'inner un carrello tornerebbe a produrre una riga sola, e una richiesta la cui risposta
     * non arriva mai sparirebbe del tutto (il cliente le vuole ingerite comunque).
     */
    @Test
    void ilJoinRestaLeftouterSullaChiaveDiSessione() {
        String query = eventsWfReqResp();

        assertTrue(query.contains("join kind=leftouter resp on SESSION_ID, TIPO_EVENTO"),
                "il broadcast della singola risposta sulle N richieste dipende da questa forma esatta");
        assertFalse(query.contains("kind=inner"),
                "con l'inner il carrello torna a 1 riga e le REQ senza risposta spariscono");
    }

    /**
     * Il moltiplicatore del lato destro deve essere 1 PER COSTRUZIONE. Se il ramo delle risposte
     * fosse deduplicato su una chiave piu' fine di quella di join, due risposte nello stesso gruppo
     * darebbero un prodotto cartesiano — e EVENTS_WF non ha vincoli di unicita' che lo intercettino,
     * quindi le righe in eccesso verrebbero scritte in silenzio.
     */
    @Test
    void ilRamoDelleRisposteEDeduplicatoSullaChiaveDiJoin() {
        String query = eventsWfReqResp();

        assertTrue(query.contains("summarize take_any(*) by SESSION_ID, TIPO_EVENTO"),
                "deduplicare le risposte sulla chiave di join rende impossibile il prodotto cartesiano");
    }

    /**
     * Il ramo delle richieste va invece deduplicato per (UNIQUE_ID, TOKEN): e' quello che distingue
     * il x2 di ADX — duplicati esatti, stesso token — dallo split legittimo del carrello, dove le
     * righe condividono l'UNIQUE_ID ma hanno token diversi. Deduplicare per solo UNIQUE_ID
     * ricollasserebbe il carrello, cioe' rifarebbe il difetto appena corretto.
     */
    @Test
    void ilRamoDelleRichiesteDeduplicaSenzaSchiacciareIlCarrello() {
        String query = eventsWfReqResp();

        assertTrue(query.contains("summarize take_any(*) by UNIQUE_ID, TOKEN_NORM"),
                "il token nella chiave e' cio' che salva lo split del carrello dalla deduplica");
        assertEquals(2, occorrenzeDi(query, "summarize take_any(*)"),
                "servono entrambi i rami: deduplicare solo le richieste lascerebbe passare il x2 sulle risposte");
    }

    /**
     * In KQL una colonna non qualificata e' quella del lato SINISTRO. Il TOKEN deve venire dalla
     * REQ, che e' l'unico campo valido su ogni riga del carrello. Se diventasse {@code TOKEN1} le N
     * righe avrebbero tutte il token della risposta, cioe' lo stesso: il difetto tornerebbe
     * identico, con l'aria di essere stato risolto.
     */
    @Test
    void ilTokenInUscitaVieneDalLatoRichiesta() {
        String query = eventsWfReqResp();

        assertTrue(query.contains(", TOKEN,"),
                "TOKEN non qualificato = lato REQ, l'unico valido su ogni riga del carrello");
        assertFalse(query.contains("TOKEN=TOKEN1") || query.contains("TOKEN = TOKEN1"),
                "TOKEN1 e' il lato RESP: tutte le N righe avrebbero lo stesso token");
    }

    // ----------------------------------------------------------------------------------
    // POSITION_TOKENS — la commissione appartiene al tentativo, non alla posizione
    // ----------------------------------------------------------------------------------

    /**
     * La lookup della FEE era indicizzata per (NAV, PA_EMITTENTE), quindi la commissione di un
     * pagamento riuscito veniva attribuita a tutti i tentativi sulla stessa posizione, anche a
     * quelli falliti. Misurato sul 23/03/2026: 527.343 token con commissione contro 95.782
     * incassati, cioe' lo stesso valore ripetuto in media su 5,5 tentativi.
     */
    @Test
    void laCommissioneEAgganciataAlTokenNonAllaPosizione() {
        String query = positionTokens();

        assertTrue(query.contains("summarize arg_min(INSERTED_TIMESTAMP, FEE_SOURCE) by TOKEN_KEY"),
                "la fee e' del tentativo di pagamento, quindi la chiave e' il token");
        assertTrue(query.contains("join kind=leftouter fee_by_token on TOKEN_KEY"),
                "l'aggancio deve usare la stessa chiave della lookup");
        assertFalse(query.contains("by NAV_KEY, PA_KEY"),
                "indicizzare per posizione ripete la stessa commissione su ogni tentativo");
    }

    /**
     * Il filtro sulle repliche NON e' un dettaglio: la commissione di un carrello e' unica, ma ADX
     * la replica identica su tutte le N righe, con token diversi. Senza il filtro ciascuno degli N
     * token si prenderebbe l'intera commissione e il totale verrebbe moltiplicato per N — un errore
     * piu' grave di quello appena corretto, perche' colpirebbe i pagamenti andati a buon fine.
     *
     * <p>Il confronto deve restare {@code !~ "true"} su {@code tostring}: sul master del carrello il
     * flag e' VUOTO, non {@code false}.</p>
     */
    @Test
    void leRepliccheDelCarrelloNonPortanoLaCommissione() {
        String query = positionTokens();

        assertTrue(query.contains("where tostring(IS_EVENT_MULTI_PAYMENT) !~ \"true\""),
                "senza questo filtro la commissione del carrello viene contata N volte");
    }

    /**
     * La lookup continua a leggere oltre la fine della finestra: l'esito arriva dopo l'attivazione,
     * e senza il margine la commissione non si aggancerebbe quasi mai. E' la ragione per cui esiste
     * un secondo bound temporale invece di riusare {@code end}.
     */
    @Test
    void laLookupDellaCommissioneGuardaOltreLaFineDellaFinestra() {
        String query = positionTokens();

        assertTrue(query.contains("fee_end=datetime_add('minute', 5, end)"),
                "la sendPaymentOutcome arriva dopo l'activate: senza margine la fee resta sempre nulla");
        assertTrue(query.contains("between (start .. fee_end)"),
                "il margine va usato, non solo dichiarato");
    }
}
