package it.pagopa.cruscotto.ingestion.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.boot.logging.logback.StructuredLogEncoder;
import org.springframework.core.env.Environment;
import org.springframework.mock.env.MockEnvironment;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Blocca il contratto della riga JSON prodotta in dev/uat/prod, dal punto di scrittura al JSON.
 *
 * <p><strong>Perche' esiste.</strong> La forma del JSON nasce dall'incrocio di tre pezzi —
 * {@link LogDetails} che scrive nell'MDC, l'encoder ECS di Spring Boot che lo promuove ad
 * attributi, e {@link CtxDetailsJsonCustomizer} che li riannida — e nessuno dei tre, da solo, la
 * determina. Un errore nell'incrocio non rompe niente: produce righe che <em>sembrano</em> giuste
 * e a cui manca un campo. Scrivendo la prima versione del customizer il filtro cancellava anche le
 * copie annidate, lasciando {@code ctx_details} vuoto, e dal codice non si vedeva.
 *
 * <p><strong>Perche' le asserzioni sono strutturali e non su stringa.</strong> Il primo giro di
 * questo test cercava {@code ,"rows":} nel testo e falliva su una riga <em>corretta</em>: quella
 * sottostringa compare anche dentro {@code ctx_details}. Cercare un campo "di primo livello" in una
 * stringa JSON non e' esprimibile; sull'albero lo e'. Il parsing verifica anche i <strong>tipi</strong>,
 * che da testo sarebbero indistinguibili da una stringa di cifre.
 */
class CtxDetailsJsonCustomizerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private StructuredLogEncoder encoder;
    private LoggerContext context;

    @BeforeEach
    void setUp() {
        MockEnvironment environment = new MockEnvironment();
        environment.setProperty("spring.application.name", "cruscotto-ingestor");
        environment.setProperty("spring.application.version", "9.9.9");
        environment.setProperty("logging.structured.ecs.service.environment", "prod");
        // Rispecchia application.yml: senza node-name l'encoder emette "service":{"node":{}},
        // cioe' l'oggetto vuoto che il test sulla forma deve poter distinguere da un difetto vero.
        environment.setProperty("logging.structured.ecs.service.node-name", "pod-7");
        environment.setProperty("logging.structured.json.customizer",
                CtxDetailsJsonCustomizer.class.getName());

        context = new LoggerContext();
        context.putObject(Environment.class.getName(), environment);

        encoder = new StructuredLogEncoder();
        encoder.setFormat("ecs");
        encoder.setCharset(StandardCharsets.UTF_8);
        encoder.setContext(context);
        encoder.start();
    }

    @AfterEach
    void cleanUp() {
        MDC.clear();
    }

    /**
     * Cattura l'MDC come lo vede logback dopo che {@link LogDetails} ha scritto: il test parte
     * dall'API reale e non da chiavi costruite a mano, altrimenti non verificherebbe il pezzo piu'
     * facile da sbagliare, cioe' la scelta del prefisso.
     */
    private Map<String, String> writtenBy(Consumer<LogDetails> fill) {
        try (LogDetails details = LogDetails.open()) {
            fill.accept(details);
            Map<String, String> snapshot = MDC.getCopyOfContextMap();
            return snapshot == null ? new HashMap<>() : new HashMap<>(snapshot);
        }
    }

    private String encodeRaw(Map<String, String> mdc, Throwable cause) {
        LoggingEvent event = new LoggingEvent();
        event.setLoggerContext(context);
        event.setLoggerName("it.pagopa.cruscotto.ingestion.service.adx.AdxClientImpl");
        event.setLevel(cause == null ? Level.INFO : Level.ERROR);
        event.setTimeStamp(1778925228123L);
        event.setMessage("Queried ADX window");
        event.setMDCPropertyMap(mdc);
        if (cause != null) {
            event.setThrowableProxy(new ThrowableProxy(cause));
        }
        return new String(encoder.encode(event), StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> encode(Map<String, String> mdc, Throwable cause) {
        try {
            return MAPPER.readValue(encodeRaw(mdc, cause), Map.class);
        } catch (Exception e) {
            throw new AssertionError("la riga prodotta non e' JSON valido", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> details(Map<String, Object> json) {
        Object nested = json.get(LogFields.CTX_DETAILS);
        assertInstanceOf(Map.class, nested, "ctx_details non e' un oggetto JSON: " + nested);
        return (Map<String, Object>) nested;
    }

    private Map<String, String> adxAttemptMdc() {
        Map<String, String> mdc = writtenBy(d -> d
                .with(LogFields.DEPENDENCY, "ADX")
                .with(LogFields.DATABASE, "re")
                .with(LogFields.ATTEMPT, 1)
                .with(LogFields.ROWS, 42L)
                .with(LogFields.ELAPSED_MS, 123L));
        mdc.put(LogFields.CORRELATION_ID, "run-1");
        mdc.put(LogFields.EVENT_ACTION, "POSITION");
        mdc.put(LogFields.EVENT_OUTCOME, "success");
        return mdc;
    }

    @Test
    void iDatiVolatiliFinisconoNellOggettoAnnidato() {
        Map<String, Object> details = details(encode(adxAttemptMdc(), null));

        assertEquals("ADX", details.get(LogFields.DEPENDENCY));
        assertEquals("re", details.get(LogFields.DATABASE));
    }

    /**
     * Il difetto che il test esiste per impedire: filtrando per nome invece che per path completo,
     * le chiavi venivano rimosse <em>anche</em> da dentro {@code ctx_details}.
     */
    @Test
    void laChiaveStaSoloDentroCtxDetailsENonAncheFuori() {
        Map<String, Object> json = encode(adxAttemptMdc(), null);

        assertFalse(details(json).isEmpty(), "ctx_details e' vuoto: il filtro ha cancellato troppo");
        for (String key : json.keySet()) {
            for (String prefix : LogFields.DETAIL_PREFIXES) {
                assertFalse(key.startsWith(prefix),
                        "la chiave tecnica '" + key + "' e' finita nel JSON invece di essere annidata");
            }
        }
    }

    /** Il prefisso tecnico non deve comparire nemmeno dentro l'oggetto annidato. */
    @Test
    void ilPrefissoTecnicoNonTrapelaNeiNomiAnnidati() {
        for (String key : details(encode(adxAttemptMdc(), null)).keySet()) {
            for (String prefix : LogFields.DETAIL_PREFIXES) {
                assertFalse(key.startsWith(prefix), "nome non ripulito dal prefisso: " + key);
            }
        }
    }

    /**
     * Il difetto visto in dev sulla 2.0.7, che la versione precedente di questo test non coglieva.
     *
     * <p>Con i prefissi {@code d.s.} / {@code d.n.} l'encoder ECS <strong>annidava sui punti</strong>:
     * {@code d.s.dependency} diventava {@code d} → {@code s} → {@code dependency}. Il filtro
     * rimuoveva la foglia ma i due padri erano gia' stati aperti, e nel JSON restava
     * {@code "d": {"s": {}, "n": {}}} — due oggetti vuoti di primo livello.
     *
     * <p>Il test di prima non lo vedeva perche' guardava solo se una chiave di primo livello
     * <em>iniziava</em> con il prefisso: la chiave di primo livello era {@code "d"}, che non
     * inizia per {@code "d.s."}. Qui si asserisce sulla forma — nessun oggetto vuoto — che e'
     * l'artefatto vero, indipendentemente da come si chiami.
     */
    @Test
    void nessunOggettoVuotoNelJson() {
        assertNoEmptyObject("", encode(adxAttemptMdc(), null));
    }

    /**
     * La ricerca e' <strong>ricorsiva</strong> di proposito. La prima versione guardava solo il
     * primo livello e non coglieva il difetto che doveva cogliere: con i prefissi puntati il JSON
     * conteneva {@code "d": {"s": {}, "n": {}}}, e {@code d} non e' vuoto — contiene {@code s} e
     * {@code n}. Il vuoto stava un livello piu' sotto, cioe' a una profondita' qualunque, che e'
     * esattamente il motivo per cui l'asserzione non puo' fermarsi alla radice.
     */
    private static void assertNoEmptyObject(String path, Map<?, ?> node) {
        node.forEach((key, value) -> {
            String childPath = path.isEmpty() ? String.valueOf(key) : path + "." + key;
            if (value instanceof Map<?, ?> nested) {
                assertFalse(nested.isEmpty(),
                        "oggetto vuoto in '" + childPath + "': e' il residuo di una chiave annidata "
                                + "dall'encoder e poi filtrata. Vedi LogFields#DETAIL_TEXT_PREFIX");
                assertNoEmptyObject(childPath, nested);
            }
        });
    }

    /** I prefissi non devono contenere punti, altrimenti l'encoder li annida. */
    @Test
    void iPrefissiNonContengonoPunti() {
        for (String prefix : LogFields.DETAIL_PREFIXES) {
            assertFalse(prefix.contains("."),
                    "il prefisso '" + prefix + "' contiene un punto: l'encoder ECS lo trasformerebbe "
                            + "in annidamento e lascerebbe oggetti vuoti nel JSON");
        }
    }

    /**
     * Se i numeri arrivassero come stringa, Elasticsearch li indicizzerebbe come keyword e ogni
     * {@code sum}/{@code avg} restituirebbe zero <strong>senza errore</strong>. E' il dato con cui
     * si misura il carico su ADX, quindi vale un test dedicato.
     */
    @Test
    void iContatoriSonoNumeriENonStringhe() {
        Map<String, Object> details = details(encode(adxAttemptMdc(), null));

        assertInstanceOf(Number.class, details.get(LogFields.ATTEMPT), "attempt non e' numerico");
        assertInstanceOf(Number.class, details.get(LogFields.ROWS), "rows non e' numerico");
        assertInstanceOf(Number.class, details.get(LogFields.ELAPSED_MS), "elapsed_ms non e' numerico");
        assertEquals(42, ((Number) details.get(LogFields.ROWS)).intValue());
    }

    /** Il tipo lo decide il tipo Java al punto di chiamata, non una tabella da aggiornare. */
    @Test
    void ilTipoSegueIlTipoJava() {
        Map<String, String> mdc = writtenBy(d -> d
                .with("un_numero", 7)
                .with("un_booleano", true)
                .with("un_testo", "sette")
                .with("un_istante", Instant.ofEpochMilli(0)));

        Map<String, Object> details = details(encode(mdc, null));

        assertInstanceOf(Number.class, details.get("un_numero"));
        assertInstanceOf(Boolean.class, details.get("un_booleano"));
        assertInstanceOf(String.class, details.get("un_testo"));
        assertInstanceOf(String.class, details.get("un_istante"), "un Instant va reso come testo");
    }

    /** Gli attributi su cui si filtra restano di primo livello, non annidati. */
    @Test
    void gliAttributiIndicizzatiRestanoDiPrimoLivello() {
        Map<String, Object> json = encode(adxAttemptMdc(), null);

        assertEquals("run-1", json.get(LogFields.CORRELATION_ID));
        assertEquals("POSITION", json.get(LogFields.EVENT_ACTION));
        assertEquals("success", json.get(LogFields.EVENT_OUTCOME));
    }

    /** Il messaggio resta la stringa statica che descrive la milestone. */
    @Test
    void ilMessaggioEstatico() {
        assertEquals("Queried ADX window", encode(adxAttemptMdc(), null).get("message"));
    }

    /**
     * Un valore assente non deve produrre una chiave: {@code rows} su un tentativo fallito non e'
     * "zero righe", e' un dato che non esiste, e uno 0 sarebbe una misura falsa.
     */
    @Test
    void unValoreNulloNonScriveLaChiave() {
        Map<String, String> mdc = writtenBy(d -> d
                .with(LogFields.DEPENDENCY, "ADX")
                .with(LogFields.ROWS, null));

        assertFalse(details(encode(mdc, null)).containsKey(LogFields.ROWS));
    }

    /** Senza nessun dato volatile l'oggetto non viene emesso affatto, invece di uscire vuoto. */
    @Test
    void senzaDatiVolatiliLOggettoNonVieneEmesso() {
        Map<String, String> mdc = new HashMap<>();
        mdc.put(LogFields.CORRELATION_ID, "run-1");

        assertFalse(encode(mdc, null).containsKey(LogFields.CTX_DETAILS));
    }

    /**
     * I dettagli valgono per una riga sola: usciti dallo scope non devono restare attaccati al
     * thread, che nei pool Quartz e' riusato dal job successivo.
     */
    @Test
    void iDettagliNonSopravvivonoAlloScope() {
        writtenBy(d -> d.with(LogFields.ROWS, 42L));

        Map<String, String> after = MDC.getCopyOfContextMap();

        assertTrue(after == null || after.isEmpty(), "dettagli rimasti nell'MDC: " + after);
    }

    /**
     * Un valore numerico malformato non deve costare la riga di log: resta stringa. Non puo'
     * arrivare da {@link LogDetails}, dove il tipo viene dal tipo Java, ma puo' arrivare da un
     * {@code MDC.put} scritto a mano, e perdere il log per un difetto di formattazione
     * significherebbe perderlo proprio quando serve.
     */
    @Test
    void unNumeroMalformatoNonFaSaltareLaRiga() {
        Map<String, String> mdc = new HashMap<>();
        mdc.put(LogFields.DETAIL_NUMERIC_PREFIX + LogFields.ROWS, "non-un-numero");

        assertEquals("non-un-numero", details(encode(mdc, null)).get(LogFields.ROWS));
    }

    /** Un decimale resta un numero e non ricade su stringa. */
    @Test
    void unDecimaleRestaNumero() {
        Map<String, String> mdc = writtenBy(d -> d.with("percentuale", 12.5));

        assertInstanceOf(Number.class, details(encode(mdc, null)).get("percentuale"));
    }

    /** Sul fallimento l'errore e' strutturato dall'encoder, non concatenato nel messaggio. */
    @Test
    void sulFallimentoLErroreEstrutturato() {
        Map<String, String> mdc = writtenBy(d -> d
                .with(LogFields.DEPENDENCY, "ADX")
                .with(LogFields.ERROR_DETAIL, "E_RS_CANNOT_RETRIEVE_VALUES_ERROR"));
        mdc.put(LogFields.EVENT_OUTCOME, "failure");

        Map<String, Object> json = encode(mdc, new IllegalStateException("cluster pieno"));

        assertNotNull(json.get("error"), "manca error.*");
        assertTrue(encodeRaw(mdc, new IllegalStateException("cluster pieno")).contains("IllegalStateException"),
                "manca il tipo dell'eccezione");
        assertEquals("failure", json.get(LogFields.EVENT_OUTCOME));
        // il segnale vero di Kusto non sta nella catena delle cause: vedi LogFields#ERROR_DETAIL
        assertEquals("E_RS_CANNOT_RETRIEVE_VALUES_ERROR", details(json).get(LogFields.ERROR_DETAIL));
    }

    /** Il formato resta una riga sola: e' il requisito che evita la frammentazione degli stack. */
    @Test
    void laRigaRestaUnaSola() {
        String raw = encodeRaw(adxAttemptMdc(), new IllegalStateException("boom"));

        assertEquals(1, raw.trim().lines().count(), "la riga JSON e' stata spezzata");
    }
}
