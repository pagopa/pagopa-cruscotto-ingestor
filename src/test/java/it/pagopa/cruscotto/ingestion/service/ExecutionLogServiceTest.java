package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExecutionLogServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private DbSchemaConfig dbSchemaConfig;

    private ExecutionLogService executionLogService;

    @BeforeEach
    void setUp() {
        when(dbSchemaConfig.getSchemaName()).thenReturn("ingestor");
        executionLogService = new ExecutionLogService(jdbcTemplate, dbSchemaConfig);
        ReflectionTestUtils.setField(executionLogService, "enabled", true);
    }

    @Test
    void logFailedShouldBindAllUpdateParameters() {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        RunContext ctx = new RunContext("POSITION_TOKENS", "run-1", Instant.now());
        ctx.incrementAnagraficaLookupCount();
        ctx.incrementPositionLookupCount();
        ctx.incrementTokenLookupCount();
        ctx.incrementCacheHitCount();
        ctx.incrementCacheMissCount();
        ctx.incrementAdxWindowCount();
        ctx.addAdxAttemptCount(2);
        ctx.incrementEmptyWindowCount();

        executionLogService.logFailed(
                ctx,
                "ERR",
                "boom",
                10,
                8,
                3,
                2,
                1,
                5,
                4
        );

        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate, times(1)).update(anyString(), argsCaptor.capture());
        // +2 for WINDOW_PROFILE and RESOLVED_MAX_DURATION_MS diagnostics columns.
        assertEquals(32, argsCaptor.getValue().length);
    }

    @Test
    void logCompletedPersistsWindowProfileAndResolvedMaxDuration() {
        // Diagnostics for the EVENTS_WF catch-up path: these must be readable from the table.
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        RunContext ctx = new RunContext("EVENTS_WF", "run-diag", Instant.now());
        ctx.setWindowProfile("CATCH_UP");
        ctx.setResolvedMaxDurationMs(3_600_000L);

        executionLogService.logCompleted(ctx, 10, 8, 3, 2, 1, 5, 4, "COMPLETED");

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate, times(1)).update(sqlCaptor.capture(), argsCaptor.capture());
        assertTrue(sqlCaptor.getValue().contains("WINDOW_PROFILE = ?"), sqlCaptor.getValue());
        assertTrue(sqlCaptor.getValue().contains("RESOLVED_MAX_DURATION_MS = ?"), sqlCaptor.getValue());
        assertTrue(java.util.Arrays.asList(argsCaptor.getValue()).contains("CATCH_UP"),
                "the window profile value must be bound");
        assertTrue(java.util.Arrays.asList(argsCaptor.getValue()).contains(3_600_000L),
                "the resolved max-duration value must be bound");
    }

    @Test
    void heartbeatUpdatesOnlyStartedRow() {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        RunContext ctx = new RunContext("POSITION_TOKENS", "run-hb", Instant.now());

        executionLogService.heartbeat(ctx, 10, 8, 3, 2, 1, 5, 4);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(1)).update(sqlCaptor.capture(), any(Object[].class));
        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("LAST_HEARTBEAT_AT"), sql);
        assertTrue(sql.contains("STATUS = 'STARTED'"), sql);
    }

    @Test
    void markOrphanedRunsAbortedReapsStaleStartedRows() {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(2);

        int reaped = executionLogService.markOrphanedRunsAborted(Duration.ofMinutes(15));

        assertEquals(2, reaped);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(1)).update(sqlCaptor.capture(), any(Object[].class));
        String sql = sqlCaptor.getValue();
        assertTrue(sql.contains("STATUS = 'ABORTED'"), sql);
        assertTrue(sql.contains("WHERE STATUS = 'STARTED'"), sql);
    }

    @Test
    void markOrphanedRunsAbortedSkipsWhenThresholdMissing() {
        assertEquals(0, executionLogService.markOrphanedRunsAborted(null));
        assertEquals(0, executionLogService.markOrphanedRunsAborted(Duration.ZERO));
        verify(jdbcTemplate, never()).update(anyString(), any(Object[].class));
    }

    // These two methods (like heartbeat/logFailed) capture best-effort runtime metrics before writing.
    // The metrics capture must never throw and prevent the primary execution-log write, regardless of
    // the JVM/runtime the platform MXBeans behave differently on. Verifying the write still happens.
    @Test
    void logStartedShouldInsertExecutionRow() {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        RunContext ctx = new RunContext("POSITION", "run-start", Instant.now());

        executionLogService.logStarted(ctx, "positionJob");

        verify(jdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }

    @Test
    void logCompletedShouldUpdateExecutionRow() {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        RunContext ctx = new RunContext("POSITION", "run-done", Instant.now());

        executionLogService.logCompleted(ctx, 10, 8, 6, 2, 1, 3, 1, "COMPLETED");

        verify(jdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }

    @Test
    void updateLatestCheckpointSkipsWhenTimestampNull() {
        RunContext ctx = new RunContext("POSITION", "run-cp", Instant.now());

        executionLogService.updateLatestCheckpoint(ctx, null);

        verify(jdbcTemplate, never()).update(anyString(), any(Object[].class));
    }

    @Test
    void updateLatestCheckpointUpdatesRowWhenTimestampPresent() {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        RunContext ctx = new RunContext("POSITION", "run-cp", Instant.now());

        executionLogService.updateLatestCheckpoint(ctx, Instant.now());

        verify(jdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }

    @Test
    void updateRunWindowSkipsWhenBoundsNull() {
        RunContext ctx = new RunContext("POSITION", "run-win", Instant.now());

        executionLogService.updateRunWindow(ctx, null, Instant.now());
        executionLogService.updateRunWindow(ctx, Instant.now(), null);

        verify(jdbcTemplate, never()).update(anyString(), any(Object[].class));
    }

    @Test
    void updateRunWindowUpdatesRowWhenBoundsPresent() {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        RunContext ctx = new RunContext("POSITION", "run-win", Instant.now());

        executionLogService.updateRunWindow(ctx, Instant.now(), Instant.now());

        verify(jdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }
}
