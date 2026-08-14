package it.pagopa.cruscotto.ingestion.ingestor;

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
 * Verifies the unique restart markers are actually emitted, so a single Elastic query on
 * {@code APP_STARTUP} / {@code APP_SHUTDOWN} reliably surfaces (re)starts and graceful stops.
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

    @Test
    void emitsUniqueStartupMarker() {
        startupLogger.onApplicationReady();
        assertTrue(appender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("APP_STARTUP")),
                "startup must emit the APP_STARTUP marker");
    }

    @Test
    void emitsUniqueShutdownMarker() {
        startupLogger.onContextClosed();
        assertTrue(appender.list.stream().anyMatch(e -> e.getFormattedMessage().contains("APP_SHUTDOWN")),
                "graceful shutdown must emit the APP_SHUTDOWN marker");
    }
}
