package it.pagopa.cruscotto.ingestion.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.logging.logback.StructuredLogEncoder;
import org.springframework.core.env.Environment;
import org.springframework.mock.env.MockEnvironment;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Blocca il contratto della riga JSON prodotta in dev/uat/prod.
 *
 * <p><strong>Perche' esiste.</strong> La forma del JSON nasce dall'incrocio di tre pezzi — encoder
 * ECS di Spring Boot, {@link CtxDetailsJsonCustomizer} e le chiavi messe in MDC dal codice — e
 * nessuno dei tre, da solo, la determina. Un errore nell'incrocio non rompe niente: produce righe
 * che <em>sembrano</em> giuste e a cui manca un campo. Durante la scrittura del customizer il primo
 * filtro cancellava anche le copie annidate, lasciando {@code ctx_details} vuoto, e la cosa era
 * invisibile dal codice.
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
        Map<String, String> mdc = new HashMap<>();
        mdc.put(LogFields.CORRELATION_ID, "run-1");
        mdc.put(LogFields.EVENT_ACTION, "POSITION");
        mdc.put(LogFields.EVENT_OUTCOME, "success");
        mdc.put(LogFields.DEPENDENCY, "ADX");
        mdc.put(LogFields.DATABASE, "re");
        mdc.put(LogFields.ATTEMPT, "1");
        mdc.put(LogFields.ROWS, "42");
        mdc.put(LogFields.ELAPSED_MS, "123");
        return mdc;
    }

    /** La classificazione dei campi deve essere completa: vedi {@link LogFields#misclassifiedKeys()}. */
    @Test
    void ilDizionarioDeiCampiEcoerente() {
        assertEquals(Set.of(), LogFields.misclassifiedKeys(),
                "chiavi senza tipo dichiarato o in entrambi gli insiemi: verrebbero perse in silenzio");
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
        for (String key : LogFields.DETAILS) {
            assertFalse(json.containsKey(key), "'" + key + "' duplicata di primo livello");
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

    @Test
    void iBooleaniSonoBooleani() {
        Map<String, String> mdc = adxAttemptMdc();
        mdc.put(LogFields.WILL_RETRY, "false");

        Object willRetry = details(encode(mdc, null)).get(LogFields.WILL_RETRY);

        assertInstanceOf(Boolean.class, willRetry, "will_retry non e' booleano");
        assertEquals(Boolean.FALSE, willRetry);
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
    void unaChiaveAssenteNonCompare() {
        Map<String, String> mdc = adxAttemptMdc();
        mdc.remove(LogFields.ROWS);

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
     * Un valore numerico malformato non deve costare la riga di log: resta stringa. Perdere il log
     * per un difetto di formattazione significherebbe perdere la traccia proprio quando serve.
     */
    @Test
    void unNumeroMalformatoNonFaSaltareLaRiga() {
        Map<String, String> mdc = adxAttemptMdc();
        mdc.put(LogFields.ROWS, "non-un-numero");

        assertEquals("non-un-numero", details(encode(mdc, null)).get(LogFields.ROWS));
    }

    /** Sul fallimento l'errore e' strutturato dall'encoder, non concatenato nel messaggio. */
    @Test
    void sulFallimentoLErroreEstrutturato() {
        Map<String, String> mdc = adxAttemptMdc();
        mdc.put(LogFields.EVENT_OUTCOME, "failure");
        mdc.put(LogFields.ERROR_DETAIL, "E_RS_CANNOT_RETRIEVE_VALUES_ERROR");

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
