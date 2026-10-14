package it.pagopa.cruscotto.ingestion.ingestor;

import it.pagopa.cruscotto.ingestion.util.LogFields;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifica che i marcatori di (ri)avvio e di arresto pulito vengano emessi, cosi' che una sola
 * ricerca su Elastic li faccia emergere.
 *
 * <p>Il marcatore e' passato dal <strong>testo</strong> del messaggio all'attributo
 * {@code ctx_phase}: una ricerca per campo invece che per sottostringa. Il test segue lo stesso
 * spostamento e asserisce sull'MDC dell'evento — e nel farlo conferma la regola per cui esiste:
 * finche' il marcatore stava nel testo, qualunque riscrittura del messaggio lo avrebbe perso in
 * silenzio.</p>
 */
class StartupLoggerTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;
    private StartupLogger startupLogger;

    @BeforeEach
    void setUp() {
        Environment environment = mock(Environment.class);
        when(environment.getActiveProfiles()).thenReturn(new String[]{"test"});
        startupLogger = new StartupLogger(environment);

        logger = (Logger) LoggerFactory.getLogger(StartupLogger.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
    }

    private boolean hasPhase(String phase) {
        return appender.list.stream()
                .anyMatch(e -> phase.equals(e.getMDCPropertyMap().get(LogFields.CTX_PHASE)));
    }

    @Test
    void emitsUniqueStartupMarker() {
        startupLogger.onApplicationReady();
        assertTrue(hasPhase("APP_STARTUP"), "l'avvio deve emettere la fase APP_STARTUP");
    }

    @Test
    void emitsUniqueShutdownMarker() {
        startupLogger.onContextClosed();
        assertTrue(hasPhase("APP_SHUTDOWN"), "l'arresto pulito deve emettere la fase APP_SHUTDOWN");
    }

    /** L'istanza serve a distinguere quale pod si e' riavviato: senza, il marcatore non basta. */
    @Test
    void ilMarcatorePortaLIstanza() {
        startupLogger.onApplicationReady();

        assertTrue(appender.list.stream()
                        .anyMatch(e -> e.getMDCPropertyMap().get(LogFields.CTX_INSTANCE_ID) != null),
                "manca ctx_instance_id sul marcatore di avvio");
    }
}
