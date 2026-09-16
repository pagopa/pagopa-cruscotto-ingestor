package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.JobExecutionException;
import org.springframework.dao.CannotAcquireLockException;

import java.sql.SQLException;
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
}
