package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.JobExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
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
