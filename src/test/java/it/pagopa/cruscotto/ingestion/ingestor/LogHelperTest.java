package it.pagopa.cruscotto.ingestion.ingestor;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Il contratto di {@link LogHelper}, che e' il punto da cui passa il percorso di ingestione.
 *
 * <p><strong>Perche' si asserisce sull'MDC dell'evento e non sul testo.</strong> Da quando il
 * contesto non sta piu' nel messaggio, un errore in questa classe non produce un messaggio
 * sbagliato: produce una riga a cui <em>manca un attributo</em>, che a occhio sembra corretta e in
 * Kibana semplicemente non si trova. {@code ILoggingEvent.getMDCPropertyMap()} e' la fotografia
 * dell'MDC nell'istante dell'emissione — l'unica cosa che conta, visto che lo scope viene chiuso
 * subito dopo.
 */
class LogHelperTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(LogHelper.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
        appender.stop();
        MDC.clear();
    }

    private static RunContext ctx() {
        return new RunContext("POSITION", "run-1", Instant.EPOCH);
    }

    private ILoggingEvent single() {
        assertEquals(1, appender.list.size(), "attesa una sola riga di log");
        return appender.list.get(0);
    }

    /**
     * Il difetto per cui questa classe e' stata riscritta: il messaggio portava davanti circa 96
     * caratteri di contesto che il JSON emette gia' come attributi. Contenendo il runId, ogni
     * messaggio era unico e non aggregabile.
     */
    @Test
    void ilMessaggioNonPortaPiuIlContestoDavanti() {
        LogHelper.info(ctx(), RunPhase.WINDOW, "Read source window");

        assertEquals("Read source window", single().getFormattedMessage());
    }

    @Test
    void ilContestoEsceComeAttributi() {
        LogHelper.info(ctx(), RunPhase.WINDOW, "Read source window");

        Map<String, String> mdc = single().getMDCPropertyMap();

        assertEquals("run-1", mdc.get(LogFields.CORRELATION_ID));
        assertEquals("POSITION", mdc.get(LogFields.EVENT_ACTION));
        assertEquals("WINDOW", mdc.get(LogFields.CTX_PHASE));
    }

    /**
     * {@code event_outcome} esiste per cercare i fallimenti. Dichiararlo {@code success} su fasi
     * che un esito non ce l'hanno — {@code START} e' un'intenzione, {@code WINDOW} un passaggio —
     * renderebbe il campo inservibile proprio per l'unica cosa a cui serve.
     */
    @Test
    void lEsitoEpresenteSoloDoveSignifichiQualcosa() {
        LogHelper.info(ctx(), RunPhase.END, "Completed ingestion run");
        assertEquals("success", single().getMDCPropertyMap().get(LogFields.EVENT_OUTCOME));

        appender.list.clear();
        LogHelper.error(ctx(), RunPhase.ERROR, "Failed ingestion run");
        assertEquals("failure", single().getMDCPropertyMap().get(LogFields.EVENT_OUTCOME));

        appender.list.clear();
        LogHelper.info(ctx(), RunPhase.BULK_OK, "Wrote payload chunk");
        assertEquals("success", single().getMDCPropertyMap().get(LogFields.EVENT_OUTCOME));

        appender.list.clear();
        LogHelper.error(ctx(), RunPhase.BULK_KO_TOTAL, "Failed to write payload chunk");
        assertEquals("failure", single().getMDCPropertyMap().get(LogFields.EVENT_OUTCOME));

        appender.list.clear();
        LogHelper.info(ctx(), RunPhase.START, "Started ingestion run");
        assertNull(single().getMDCPropertyMap().get(LogFields.EVENT_OUTCOME),
                "START e' un'intenzione: non ha un esito");

        appender.list.clear();
        LogHelper.info(ctx(), RunPhase.WINDOW, "Read source window");
        assertNull(single().getMDCPropertyMap().get(LogFields.EVENT_OUTCOME),
                "WINDOW e' un passaggio del ciclo: non ha un esito");
    }

    /** Le fasi passate come stringa libera non hanno un esito da dedurre. */
    @Test
    void unaFaseLiberaNonInventaUnEsito() {
        LogHelper.info(ctx(), "OPERATION_SUMMARY", "Completed operation cycle");

        Map<String, String> mdc = single().getMDCPropertyMap();

        assertEquals("OPERATION_SUMMARY", mdc.get(LogFields.CTX_PHASE));
        assertNull(mdc.get(LogFields.EVENT_OUTCOME));
    }

    /**
     * Un {@code operationId} assente non deve produrre la chiave: in Kibana un
     * {@code ctx_operation_id} valorizzato a {@code "null"} si filtra come un valore vero, e le
     * righe fuori da un ciclo verrebbero contate come se ne avessero uno.
     */
    @Test
    void unOperationIdAssenteNonProduceLaChiave() {
        LogHelper.info(ctx(), RunPhase.START, "Started ingestion run");

        assertFalse(single().getMDCPropertyMap().containsKey(LogFields.CTX_OPERATION_ID));
    }

    @Test
    void unOperationIdVuotoVieneTrattatoComeAssente() {
        RunContext ctx = ctx();
        ctx.setOperationId("   ");

        LogHelper.info(ctx, RunPhase.WINDOW, "Read source window");

        assertFalse(single().getMDCPropertyMap().containsKey(LogFields.CTX_OPERATION_ID));
    }

    @Test
    void unOperationIdPresenteEsceComeAttributo() {
        RunContext ctx = ctx();
        ctx.setOperationId("op-7");

        LogHelper.info(ctx, RunPhase.WINDOW, "Read source window");

        assertEquals("op-7", single().getMDCPropertyMap().get(LogFields.CTX_OPERATION_ID));
    }

    /**
     * {@link LogHelper} riscrive il contesto a ogni chiamata, anche dentro uno scope che lo ha gia'
     * impostato. Deve <strong>ripristinarlo</strong>, non cancellarlo: i thread dei pool Quartz
     * sono riusati, e una chiave rimasta attribuirebbe al job successivo un contesto non suo.
     */
    @Test
    void nonRubaIlContestoAlChiamante() {
        try (MdcScope ignored = MdcScope.open()
                .with(LogFields.CORRELATION_ID, "run-esterno")
                .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH")) {

            LogHelper.info(ctx(), RunPhase.WINDOW, "Read source window");

            assertEquals("run-esterno", MDC.get(LogFields.CORRELATION_ID));
            assertEquals("MASSIVE_SEARCH", MDC.get(LogFields.EVENT_ACTION));
        }
        assertNull(MDC.get(LogFields.CTX_PHASE), "la fase non deve sopravvivere alla chiamata");
    }

    /** I dettagli aperti dal chiamante devono arrivare alla riga emessa da LogHelper. */
    @Test
    void iDettagliDelChiamanteArrivanoAllaRiga() {
        try (LogDetails ignored = LogDetails.open().with(LogFields.ROWS, 42L)) {
            LogHelper.info(ctx(), RunPhase.WINDOW, "Read source window");
        }

        assertEquals("42",
                single().getMDCPropertyMap().get(LogFields.DETAIL_NUMERIC_PREFIX + LogFields.ROWS));
    }

    /** Lo stack arriva al logger come throwable, non concatenato: lo richiede il livello ERROR. */
    @Test
    void lEccezioneArrivaAlLoggerComeThrowable() {
        LogHelper.error(ctx(), RunPhase.ERROR, "Failed ingestion run", new IllegalStateException("boom"));

        assertTrue(single().getThrowableProxy() != null, "l'eccezione non e' arrivata al logger");
        assertEquals("Failed ingestion run", single().getFormattedMessage());
    }
}
