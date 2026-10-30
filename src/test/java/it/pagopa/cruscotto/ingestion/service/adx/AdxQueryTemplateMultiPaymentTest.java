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
     * La lookup della FEE era indicizzata per (NAV, PA_EMITTENTE), quindi la commissione finiva su
     * tutti i tentativi della stessa posizione invece che sul proprio. La chiave e' il token perche'
     * la commissione e' della transazione, non della posizione debitoria.
     */
    @Test
    void laCommissioneEAgganciataAlTokenNonAllaPosizione() {
        String query = positionTokens();

        assertTrue(query.contains("summarize arg_min(INSERTED_TIMESTAMP, FEE_CLOSE) by TOKEN_KEY"),
                "anche la fonte closePayment-v2 va indicizzata per token");
        assertTrue(query.contains("summarize arg_min(INSERTED_TIMESTAMP, FEE_OUTCOME) by TOKEN_KEY"),
                "anche la fonte sendPaymentOutcome va indicizzata per token");
        assertTrue(query.contains("join kind=leftouter fee_from_close on TOKEN_KEY")
                        && query.contains("join kind=leftouter fee_from_outcome on TOKEN_KEY"),
                "entrambi gli agganci devono usare la stessa chiave delle lookup");
        assertFalse(query.contains("NAV_KEY") || query.contains("PA_KEY"),
                "indicizzare per posizione ripete la stessa commissione su ogni tentativo");
    }

    /**
     * Specifica del cliente: per {@code PAGOPA_CHECKOUT} la commissione sta sulla
     * {@code closePayment-v2}, per {@code TOUCHPOINT_PSP} sulla {@code sendPaymentOutcome}.
     *
     * <p>La scelta si fa sulla riga da arricchire e non filtrando la sorgente, perche' il
     * {@code TOUCHPOINT} <strong>non e' valorizzato sulle righe di pagamento</strong>: verificato su
     * ADX, tutte e 153.755 le righe con fee lo hanno vuoto. Sta solo sull'activate, che e' appunto
     * la riga che stiamo arricchendo.</p>
     */
    @Test
    void ilTouchpointDecideQualeFonteViene() {
        String query = positionTokens();

        assertTrue(query.contains("iff(trim(\" \", tostring(TOUCHPOINT)) =~ \"PAGOPA_CHECKOUT\""),
                "la fonte si sceglie sul touchpoint dell'activate, non filtrando le lookup");

        boolean touchpointUsatoComeFiltro = Arrays.stream(query.split("\\R"))
                .map(String::trim)
                .filter(line -> line.startsWith("| where"))
                .anyMatch(line -> line.contains("TOUCHPOINT"));
        assertFalse(touchpointUsatoComeFiltro,
                "filtrare le sorgenti per touchpoint non puo' funzionare: li' il campo e' vuoto");
    }

    /**
     * Il ripiego sull'altra fonte non e' difensivo: oggi e' la norma. La {@code closePayment-v2} non
     * porta alcuna commissione — 26.203 righe e zero con FEE, misurato su ADX — perche' e' un
     * difetto lato sorgente che il cliente sta correggendo. Senza ripiego, applicare la specifica
     * alla lettera avrebbe azzerato la fee sul 100% dei pagamenti Checkout, che oggi ce l'hanno
     * dalla sendPaymentOutcome nel 74,5% dei casi.
     */
    @Test
    void seLaFonteSceltaNonPortaLaCommissioneSiRipiegaSullAltra() {
        String query = positionTokens();

        assertTrue(query.contains("coalesce(FEE_CLOSE, FEE_OUTCOME)"),
                "su Checkout: closePayment-v2 prima, poi ripiego sulla sendPaymentOutcome");
        assertTrue(query.contains("coalesce(FEE_OUTCOME, FEE_CLOSE)"),
                "sugli altri touchpoint: sendPaymentOutcome prima, poi ripiego sulla closePayment-v2");
    }

    /**
     * Le due lookup devono leggere fonti DIVERSE. Sembra ovvio, ma sono due blocchi quasi identici
     * copiati uno sull'altro: bastano due caratteri perche' entrambe leggano la sendPaymentOutcome,
     * e il risultato sarebbe una query che funziona, passa tutti gli altri test, e ignora
     * silenziosamente la closePayment-v2 — cioe' la fonte che il cliente considera autorevole per
     * i pagamenti Checkout.
     */
    @Test
    void leDueLookupLeggonoFontiDiverse() {
        String query = positionTokens();

        assertEquals(1, occorrenzeDi(query, "where TIPO_EVENTO == \"closePayment-v2\""),
                "una lookup, e una sola, deve leggere la closePayment-v2");
        assertEquals(1, occorrenzeDi(query, "where TIPO_EVENTO in (\"sendPaymentOutcome\", \"sendPaymentOutcomeV2\")"),
                "una lookup, e una sola, deve leggere la sendPaymentOutcome e la V2");
    }

    /**
     * Il valore gia' presente sulla riga vince sulla lookup, ma il confronto deve essere su
     * "vuoto", non su "null".
     *
     * <p>Gli activatePaymentNotice non portano la commissione — zero righe con FEE su 236.876,
     * misurato su ADX — e oggi quel campo arriva come null, quindi un {@code coalesce} basterebbe.
     * Ma se la sorgente passasse a emettere stringa vuota invece di null, il coalesce
     * restituirebbe la stringa vuota e la lookup verrebbe scartata: la commissione sparirebbe dal
     * 100% dei token, <strong>senza errori e senza che nessun test se ne accorga</strong>. Siccome
     * questa query e' l'unica occasione per agganciare la fee (la INSERT e' first-write-wins e
     * nessuno la aggiorna dopo), il dato sarebbe perso in modo definitivo.</p>
     */
    @Test
    void ilValoreGiaPresenteVinceMaIlConfrontoEsuVuotoNonSuNull() {
        String query = positionTokens();

        assertTrue(query.contains("iff(isempty(trim(\" \", tostring(FEE)))"),
                "con un coalesce una FEE stringa-vuota sull'activate scarterebbe la lookup in silenzio");
        assertFalse(query.contains("coalesce(FEE,"),
                "coalesce non distingue null da stringa vuota: qui la differenza vale l'intera colonna");
    }

    /**
     * Scelta esplicita del cliente: la commissione va cercata sempre, senza condizionarla all'esito.
     * Sarebbe stato comunque quasi ininfluente — fra le righe con fee, 153.529 OK contro 226 KO —
     * ma un filtro su un campo che potrebbe non essere valorizzato nello stream dei token avrebbe
     * rischiato di azzerare la copertura senza che nessun test se ne accorgesse.
     */
    @Test
    void laRicercaDellaCommissioneNonDipendeDallEsito() {
        String query = positionTokens();

        boolean filtrataSullEsito = Arrays.stream(query.split("\\R"))
                .map(String::trim)
                .filter(line -> line.startsWith("| where"))
                .anyMatch(line -> line.contains("OUTCOME"));
        assertFalse(filtrataSullEsito, "la commissione si cerca sempre, qualunque sia l'esito");
    }

    /**
     * La commissione di un carrello e' unica sull'intero paniere, e il cliente vuole che compaia su
     * TUTTI gli N tentativi: e' il flag di carrello sulla riga a dire che non va sommata.
     *
     * <p>Si ottiene non facendo nulla, perche' ADX replica la FEE su tutte le N righe dello split,
     * ciascuna col proprio token, e la lookup e' indicizzata per token. Una versione precedente
     * filtrava la sorgente con {@code IS_EVENT_MULTI_PAYMENT !~ "true"} per tenere solo la riga
     * master: il risultato era che un token del carrello prendeva la commissione e gli altri N-1
     * restavano vuoti. Questo test impedisce che quel filtro torni.</p>
     *
     * <p>Verificato su ADX sul carrello {@code 2026-03-23_-1010589750507783718}: cinque righe
     * {@code sendPaymentOutcomeV2}, token diversi, {@code FEE = 0,28} su tutte e cinque.</p>
     */
    @Test
    void laCommissioneArrivaSuTuttiITentativiDelCarrello() {
        String query = positionTokens();

        // L'asserzione e' sul FILTRO, non sulla colonna: IS_EVENT_MULTI_PAYMENT resta
        // legittimamente nella project finale, perche' e' un dato che esponiamo in uscita.
        boolean filtrataSulFlag = Arrays.stream(query.split("\\R"))
                .map(String::trim)
                .filter(line -> line.startsWith("| where"))
                .anyMatch(line -> line.contains("IS_EVENT_MULTI_PAYMENT"));

        assertFalse(filtrataSulFlag,
                "filtrare le repliche lascia la commissione del carrello su un token solo, "
                        + "mentre il requisito e' che compaia su tutti");
        assertTrue(query.contains(", IS_EVENT_MULTI_PAYMENT,"),
                "la colonna resta in uscita: serve a valle per il ramo token-first delle repliche");
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
        assertEquals(2, occorrenzeDi(query, "between (start .. fee_end)"),
                "il margine serve a entrambe le fonti: una lookup senza non aggancerebbe quasi nulla");
        assertTrue(query.contains("between (start .. fee_end)"),
                "il margine va usato, non solo dichiarato");
    }
}
