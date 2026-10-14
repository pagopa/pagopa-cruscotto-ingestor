package it.pagopa.cruscotto.ingestion.scheduler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.JobExecutionException;
import org.springframework.dao.CannotAcquireLockException;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Production has no direct application-log access, so every job failure must be readable from
 * INGEST_EXECUTION_LOG alone. These tests pin that guarantee.
 */
@ExtendWith(MockitoExtension.class)
class TrackedJobExecutorTest {

    @Mock
    private ExecutionLogService executionLogService;

    private TrackedJobExecutor executor() {
        return new TrackedJobExecutor(executionLogService);
    }

    @Test
    void runTrackedWritesStartedThenCompleted() throws Exception {
        executor().runTracked("STG_INGEST_ERROR", "quartz-STG_INGEST_ERROR", "run-1", () -> { });

        verify(executionLogService).logStarted(any(RunContext.class), eq("quartz-STG_INGEST_ERROR"));
        verify(executionLogService).logCompleted(any(RunContext.class), anyLong(), anyLong(), anyLong(),
                anyLong(), anyLong(), anyLong(), anyLong(), eq("COMPLETED"));
        verify(executionLogService, never()).logFailed(any(), anyString(), anyString(), anyString(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void runTrackedRecordsFailureAndRethrows() {
        RuntimeException boom = new IllegalStateException("cleanup exploded");

        JobExecutionException thrown = assertThrows(JobExecutionException.class,
                () -> executor().runTracked("STG_INGEST_ERROR", "quartz-STG_INGEST_ERROR", "run-1", () -> {
                    throw boom;
                }));

        assertEquals(boom, thrown.getCause());
        verify(executionLogService).logStarted(any(RunContext.class), eq("quartz-STG_INGEST_ERROR"));
        verify(executionLogService).logFailed(any(RunContext.class), eq("quartz-STG_INGEST_ERROR"),
                eq("IllegalStateException"), anyString(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
        verify(executionLogService, never()).logCompleted(any(), anyLong(), anyLong(), anyLong(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyString());
    }

    @Test
    void recordFailureStoresTheWholeCauseChain() {
        // The root cause (Postgres/ADX message) is the actionable part and must be in the table.
        Throwable root = new IllegalArgumentException("value too long for type character varying(255)");
        Throwable middle = new IllegalStateException("transform failed", root);
        Throwable top = new RuntimeException("job failed", middle);

        executor().recordFailure("POSITION", "batch-POSITION", "run-1", top);

        ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
        // ERROR_CODE must be the ROOT cause (the actionable class), not the RuntimeException wrapper.
        verify(executionLogService).logFailed(any(RunContext.class), eq("batch-POSITION"),
                eq("IllegalArgumentException"), message.capture(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());

        String persisted = message.getValue();
        assertTrue(persisted.contains("job failed"), persisted);
        assertTrue(persisted.contains("transform failed"), persisted);
        assertTrue(persisted.contains("value too long for type character varying(255)"),
                "the root cause must be readable straight from the log table: " + persisted);
    }

    @Test
    void recordFailureCarriesRunIdAndEntityName() {
        executor().recordFailure("EVENTS_WF", "batch-EVENTS_WF", "run-42", new RuntimeException("adx down"));

        ArgumentCaptor<RunContext> ctx = ArgumentCaptor.forClass(RunContext.class);
        verify(executionLogService).logFailed(ctx.capture(), eq("batch-EVENTS_WF"), anyString(), anyString(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
        assertEquals("run-42", ctx.getValue().getRunId());
        assertEquals("EVENTS_WF", ctx.getValue().getEntityName());
    }

    @Test
    void runTrackedRetriesTransientSerializationFailureThenCompletes() throws Exception {
        // Reconciliation/cleanup path: un conflitto transitorio al lancio deve essere ritentato e poi
        // completare (STARTED -> COMPLETED), senza registrare fallimenti.
        AtomicInteger attempts = new AtomicInteger();
        executor().runTracked("RECONCILIATION", "batch-RECONCILIATION", "run-1", () -> {
            if (attempts.getAndIncrement() == 0) {
                throw new CannotAcquireLockException("could not serialize access",
                        new SQLException("serialization_failure", "40001"));
            }
        });

        assertEquals(2, attempts.get());
        verify(executionLogService).logCompleted(any(RunContext.class), anyLong(), anyLong(), anyLong(),
                anyLong(), anyLong(), anyLong(), anyLong(), eq("COMPLETED"));
        verify(executionLogService, never()).logFailed(any(), anyString(), anyString(), anyString(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void runFailSafeRetriesTransientSerializationFailureThenSucceeds() throws Exception {
        // Collisione transitoria sui metadati Spring Batch (SQLSTATE 40001): deve ritentare e poi
        // completare, senza registrare alcun fallimento.
        AtomicInteger attempts = new AtomicInteger();
        executor().runFailSafe("EXTRA_INFO", "batch-EXTRA_INFO", "run-1", () -> {
            if (attempts.getAndIncrement() == 0) {
                throw new CannotAcquireLockException("could not serialize access",
                        new SQLException("serialization_failure", "40001"));
            }
        });

        assertEquals(2, attempts.get(), "must retry once then succeed");
        verify(executionLogService, never()).logFailed(any(), anyString(), anyString(), anyString(),
                anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void runFailSafeRecordsFailureOnlyAfterRetriesAreExhausted() {
        AtomicInteger attempts = new AtomicInteger();
        JobExecutionException thrown = assertThrows(JobExecutionException.class,
                () -> executor().runFailSafe("EXTRA_INFO", "batch-EXTRA_INFO", "run-1", () -> {
                    attempts.incrementAndGet();
                    throw new CannotAcquireLockException("still serializing",
                            new SQLException("serialization_failure", "40001"));
                }));

        assertEquals(3, attempts.get(), "must exhaust the 3 attempts");
        // La failure viene registrata UNA sola volta, a retry esauriti.
        verify(executionLogService, times(1)).logFailed(any(RunContext.class), eq("batch-EXTRA_INFO"),
                anyString(), anyString(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
        assertTrue(thrown.getCause() instanceof CannotAcquireLockException);
    }

    @Test
    void runFailSafeDoesNotRetryNonTransientFailures() {
        AtomicInteger attempts = new AtomicInteger();
        assertThrows(JobExecutionException.class,
                () -> executor().runFailSafe("EXTRA_INFO", "batch-EXTRA_INFO", "run-1", () -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("boom");
                }));

        assertEquals(1, attempts.get(), "a non-transient failure must not be retried");
        verify(executionLogService, times(1)).logFailed(any(RunContext.class), eq("batch-EXTRA_INFO"),
                anyString(), anyString(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void aFailingExecutionLogWriteDoesNotHideTheOriginalError() {
        // Bookkeeping must never replace the real failure: the job error still propagates.
        doThrow(new RuntimeException("log table unreachable"))
                .when(executionLogService).logFailed(any(), anyString(), anyString(), anyString(),
                        anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
        RuntimeException boom = new IllegalStateException("original failure");

        JobExecutionException thrown = assertThrows(JobExecutionException.class,
                () -> executor().runTracked("POSITION", "batch-POSITION", "run-1", () -> {
                    throw boom;
                }));

        assertEquals(boom, thrown.getCause());
    }

    /**
     * Il difetto che questo test esiste per impedire.
     *
     * <p>In {@code runFailSafe} il {@code catch} era attaccato al try-with-resources, e Java chiude
     * le risorse <strong>prima</strong> di eseguire {@code catch} e {@code finally}: quindi la riga
     * di fallimento veniva emessa con l'MDC gia' ripulito, senza {@code event_action},
     * {@code correlation_id} ne' {@code ctx_instance_id}. Era invisibile perche' quegli stessi
     * valori comparivano anche nel testo del messaggio — tolti da li', il fallimento sarebbe
     * diventato una riga senza contesto, cioe' non correlabile al run che l'ha prodotta.
     *
     * <p>Si asserisce sull'MDC <em>dell'evento</em>: e' la fotografia nell'istante dell'emissione,
     * l'unica che conti, dato che lo scope viene chiuso subito dopo.
     */
    @Test
    void ilLogDiFallimentoConservaIlContesto() {
        Logger logger = (Logger) LoggerFactory.getLogger(TrackedJobExecutor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            assertThrows(JobExecutionException.class, () ->
                    executor().runFailSafe("POSITION", "batch-POSITION", "run-9", () -> {
                        throw new IllegalStateException("boom");
                    }));

            ILoggingEvent failure = appender.list.stream()
                    .filter(e -> e.getLevel() == Level.ERROR)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("nessuna riga di errore emessa"));

            assertEquals("POSITION", failure.getMDCPropertyMap().get(LogFields.EVENT_ACTION));
            assertEquals("run-9", failure.getMDCPropertyMap().get(LogFields.CORRELATION_ID));
        } finally {
            logger.detachAppender(appender);
            appender.stop();
            MDC.clear();
        }
    }

    /** Le due righe di ciclo di vita del job sono emesse da qui, non dai dieci job Quartz. */
    @Test
    void emetteLeDueRigheDiCicloDiVitaConLaFase() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(TrackedJobExecutor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            executor().runFailSafe("POSITION", "batch-POSITION", "run-10", () -> { });

            List<String> phases = appender.list.stream()
                    .map(e -> e.getMDCPropertyMap().get(LogFields.CTX_PHASE))
                    .toList();

            assertTrue(phases.contains("START"), "manca la riga di avvio: " + phases);
            assertTrue(phases.contains("END"), "manca la riga di fine: " + phases);
            // Nessun esito su END: gira in un finally, quindi anche dopo un fallimento.
            appender.list.stream()
                    .filter(e -> "END".equals(e.getMDCPropertyMap().get(LogFields.CTX_PHASE)))
                    .forEach(e -> assertEquals(null, e.getMDCPropertyMap().get(LogFields.EVENT_OUTCOME),
                            "END non deve dichiarare un esito"));
        } finally {
            logger.detachAppender(appender);
            appender.stop();
            MDC.clear();
        }
    }
}
