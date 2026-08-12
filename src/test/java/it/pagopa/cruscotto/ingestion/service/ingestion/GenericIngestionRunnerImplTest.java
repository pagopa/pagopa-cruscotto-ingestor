package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.CheckpointStoreService;
import it.pagopa.cruscotto.ingestion.service.EndLimitResolverService;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.service.ExtraInfoWhitelistService;
import it.pagopa.cruscotto.ingestion.service.RunGuardrails;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryService;
import it.pagopa.cruscotto.ingestion.service.adx.AdxWindowResult;
import it.pagopa.cruscotto.ingestion.entity.Position;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.CannotAcquireLockException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GenericIngestionRunnerImplTest {

    @Mock
    private CheckpointStoreService checkpointStore;

    @Mock
    private EndLimitResolverService endLimitResolver;

    @Mock
    private RunGuardrails runGuardrails;

    @Mock
    private AdxQueryService adxQueryService;

    @Mock
    private OldestTimestampProvider oldestTimestampProvider;

    @Mock
    private ExecutionLogService executionLogService;

    @Mock
    private EntityTransformer entityTransformer;

    @Mock
    private WindowCyclePersistenceService windowCyclePersistenceService;

    @Mock
    private ExtraInfoWhitelistService extraInfoWhitelistService;

    @Mock
    private PositionFkBatchPrefetcher positionFkBatchPrefetcher;

    @Mock
    private TokenFkBatchPrefetcher tokenFkBatchPrefetcher;

    private GenericIngestionRunnerImpl runner;

    private IngestionConfig ingestionConfig;

    @BeforeEach
    void setUp() {
        ingestionConfig = new IngestionConfig();
        ingestionConfig.setInitialWindow(Duration.ofMinutes(5));
        ingestionConfig.setFirstRunStart(Instant.parse("2026-07-01T00:00:00Z"));

        runner = new GenericIngestionRunnerImpl(
                checkpointStore,
                endLimitResolver,
                runGuardrails,
                adxQueryService,
                oldestTimestampProvider,
                executionLogService,
                windowCyclePersistenceService,
                ingestionConfig,
                entityTransformer,
                extraInfoWhitelistService,
                positionFkBatchPrefetcher,
                tokenFkBatchPrefetcher
        );

        lenient().when(extraInfoWhitelistService.isAllowed(any())).thenReturn(true);
    }

    @Test
    void shouldStartFromConfiguredStartWhenCheckpointAndAdxOldestAreMissing() {
        Instant runStart = Instant.parse("2026-05-08T12:00:00Z");
        RunContext ctx = new RunContext("POSITION", "run-123", runStart);
        Instant endLimit = Instant.parse("2026-07-01T02:00:00Z");
        Instant expectedCursor = Instant.parse("2026-07-01T00:00:00Z");

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(any())).thenReturn(Optional.empty());
        when(oldestTimestampProvider.getOldestTimestamp(ctx, EntityName.POSITION))
                .thenReturn(Optional.empty());
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                expectedCursor,
                expectedCursor.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                new HashMap<>()
        );
        when(adxQueryService.fetchWindow(eq(ctx), any(), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));

        runner.runEntity(ctx);

        ArgumentCaptor<Instant> cursorCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(adxQueryService, times(1))
                .fetchWindow(eq(ctx), cursorCaptor.capture(), eq(Duration.ofMinutes(5)), eq(endLimit));
        assertEquals(expectedCursor, cursorCaptor.getValue());
    }

    @Test
    void shouldProbeAndJumpOverEmptyGapToNextAvailableData() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("POSITION", "run-gap", runStart);
        Instant checkpoint = runStart.minus(Duration.ofHours(2));
        Instant endLimit = checkpoint.plus(Duration.ofHours(1));
        Instant windowEnd = checkpoint.plus(Duration.ofMinutes(5));
        Instant jumpTarget = checkpoint.plus(Duration.ofMinutes(30));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, true, false);

        // First window (from the checkpoint) is empty -> the probe must be consulted.
        AdxWindowResult emptyWindow = new AdxWindowResult(
                checkpoint, windowEnd, Duration.ofMinutes(5), 1, new HashMap<>());
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));

        // Probe reports the next available data at jumpTarget: the cursor must jump straight there.
        when(adxQueryService.findNextInsertedTimestamp(eq(ctx), eq(EntityName.POSITION), eq(windowEnd), eq(endLimit)))
                .thenReturn(Optional.of(jumpTarget));

        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", jumpTarget.plusSeconds(15).toString());
        HashMap<String, Object> rows = new HashMap<>();
        rows.put("row-1", row);
        AdxWindowResult dataWindow = new AdxWindowResult(
                jumpTarget, jumpTarget.plus(Duration.ofMinutes(5)), Duration.ofMinutes(5), 1, rows);
        when(adxQueryService.fetchWindow(eq(ctx), eq(jumpTarget), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(dataWindow));

        Position transformed = new Position();
        transformed.setDateEvent(jumpTarget.atZone(ZoneOffset.UTC).toLocalDate());
        when(entityTransformer.transform(eq(row), eq(Position.class), eq(ctx), eq(EntityName.POSITION)))
                .thenReturn(transformed);
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, jumpTarget.plusSeconds(15)));

        runner.runEntity(ctx);

        // The gap was skipped via a single probe, and the data window at the jump target was processed.
        verify(adxQueryService, times(1))
                .findNextInsertedTimestamp(eq(ctx), eq(EntityName.POSITION), eq(windowEnd), eq(endLimit));
        verify(adxQueryService, times(1))
                .fetchWindow(eq(ctx), eq(jumpTarget), eq(Duration.ofMinutes(5)), eq(endLimit));
        verify(windowCyclePersistenceService, times(1))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any());
    }

    @Test
    void shouldFallBackToStepAdvanceWhenEmptyWindowProbeFails() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("POSITION", "run-probe-fail", runStart);
        Instant checkpoint = runStart.minus(Duration.ofHours(2));
        Instant endLimit = checkpoint.plus(Duration.ofHours(1));
        Instant windowEnd = checkpoint.plus(Duration.ofMinutes(5));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                checkpoint, windowEnd, Duration.ofMinutes(5), 1, new HashMap<>());
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));
        // Probe blows up -> the runner must fall back to the single-step advance, NOT jump to endLimit.
        when(adxQueryService.findNextInsertedTimestamp(eq(ctx), eq(EntityName.POSITION), eq(windowEnd), eq(endLimit)))
                .thenThrow(new IllegalStateException("boom"));

        runner.runEntity(ctx);

        // Stepped to windowEnd (checkpoint + 5min), not fast-forwarded to endLimit.
        verify(executionLogService, times(1)).updateRunWindow(eq(ctx), eq(checkpoint), eq(windowEnd));
        verify(windowCyclePersistenceService, times(0))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any());
    }

    @Test
    void shouldProbeEmptyWindowForEventsWfToo() {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("EVENTS_WF", "run-eventswf-gap", runStart);
        Instant checkpoint = runStart.minus(Duration.ofHours(2));
        Instant endLimit = checkpoint.plus(Duration.ofHours(1));
        Instant windowEnd = checkpoint.plus(Duration.ofMinutes(5));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EVENTS_WF)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                checkpoint, windowEnd, Duration.ofMinutes(5), 1, new HashMap<>());
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));
        when(adxQueryService.findNextInsertedTimestamp(eq(ctx), eq(EntityName.EVENTS_WF), eq(windowEnd), eq(endLimit)))
                .thenReturn(Optional.empty());

        runner.runEntity(ctx);

        // EVENTS_WF must consult the probe on an empty window just like the other ADX-window entities.
        verify(adxQueryService, times(1))
                .findNextInsertedTimestamp(eq(ctx), eq(EntityName.EVENTS_WF), eq(windowEnd), eq(endLimit));
    }

    @Test
    void shouldStartFromAdxOldestWhenItIsNewerThanConfiguredStart() {
        Instant runStart = Instant.parse("2026-05-08T12:00:00Z");
        RunContext ctx = new RunContext("POSITION", "run-456", runStart);
        Instant endLimit = Instant.parse("2026-07-11T10:00:00Z");
        Instant adxOldest = Instant.parse("2026-07-10T12:00:00Z");

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(any())).thenReturn(Optional.empty());
        when(oldestTimestampProvider.getOldestTimestamp(ctx, EntityName.POSITION))
                .thenReturn(Optional.of(adxOldest));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                adxOldest,
                adxOldest.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                new HashMap<>()
        );
        when(adxQueryService.fetchWindow(eq(ctx), any(), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));

        runner.runEntity(ctx);

        ArgumentCaptor<Instant> cursorCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(adxQueryService, times(1))
                .fetchWindow(eq(ctx), cursorCaptor.capture(), eq(Duration.ofMinutes(5)), eq(endLimit));
        assertEquals(adxOldest, cursorCaptor.getValue());
    }

    @Test
    void shouldClampToConfiguredStartWhenAdxOldestIsOlder() {
        Instant runStart = Instant.parse("2026-05-08T12:00:00Z");
        RunContext ctx = new RunContext("POSITION", "run-789", runStart);
        Instant endLimit = Instant.parse("2026-07-01T02:00:00Z");
        Instant configuredStart = Instant.parse("2026-07-01T00:00:00Z");
        Instant adxOldest = Instant.parse("2025-01-01T00:00:00Z");

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(any())).thenReturn(Optional.empty());
        when(oldestTimestampProvider.getOldestTimestamp(ctx, EntityName.POSITION))
                .thenReturn(Optional.of(adxOldest));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                configuredStart,
                configuredStart.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                new HashMap<>()
        );
        when(adxQueryService.fetchWindow(eq(ctx), any(), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));

        runner.runEntity(ctx);

        ArgumentCaptor<Instant> cursorCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(adxQueryService, times(1))
                .fetchWindow(eq(ctx), cursorCaptor.capture(), eq(Duration.ofMinutes(5)), eq(endLimit));
        assertEquals(configuredStart, cursorCaptor.getValue());
    }

    @Test
    void childFirstRunCursorIsFlooredToFirstRunStart() {
        // POSITION_TOKENS (child of POSITION) with no own checkpoint: its ADX data starts months
        // before the ingestion origin, but those records reference positions that were never
        // ingested (before first-run-start). The first-run cursor must therefore be floored to
        // first-run-start, not the March ADX oldest — otherwise every run re-reads/re-stages a huge
        // unresolvable backlog and never checkpoints.
        Instant runStart = Instant.parse("2026-08-06T09:00:00Z");
        RunContext ctx = new RunContext("POSITION_TOKENS", "run-floor", runStart);
        Instant firstRunStart = Instant.parse("2026-07-01T00:00:00Z"); // configured in setUp()
        Instant endLimit = Instant.parse("2026-07-01T02:00:00Z");
        Instant parentCheckpoint = Instant.parse("2026-08-01T00:00:00Z");
        Instant childAdxOldest = Instant.parse("2026-03-18T01:08:00Z");

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION_TOKENS)).thenReturn(Optional.empty());
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(parentCheckpoint));
        when(oldestTimestampProvider.getOldestTimestamp(ctx, EntityName.POSITION_TOKENS))
                .thenReturn(Optional.of(childAdxOldest));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                firstRunStart,
                firstRunStart.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                new HashMap<>()
        );
        when(adxQueryService.fetchWindow(eq(ctx), any(), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));

        runner.runEntity(ctx);

        ArgumentCaptor<Instant> cursorCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(adxQueryService, times(1))
                .fetchWindow(eq(ctx), cursorCaptor.capture(), eq(Duration.ofMinutes(5)), eq(endLimit));
        assertEquals(firstRunStart, cursorCaptor.getValue());
    }

    @Test
    void shouldStageRowsWhenTransformationFails() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("POSITION_TOKENS", "run-stage", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(5));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION_TOKENS)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", checkpoint.plusSeconds(30).toString());
        row.put("NAV", "NAV-1");

        HashMap<String, Object> rows = new HashMap<>();
        rows.put("row-1", row);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));
        when(entityTransformer.transform(eq(row), eq(it.pagopa.cruscotto.ingestion.entity.PositionTokens.class), eq(ctx), eq(EntityName.POSITION_TOKENS)))
                .thenThrow(new EntityTransformer.TransformationException("Missing required FK fkPosition"));
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION_TOKENS), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(0, 1, checkpoint.plusSeconds(30)));

        runner.runEntity(ctx);

        verify(windowCyclePersistenceService, times(1))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION_TOKENS), any(), any(), any(), any());
        verify(executionLogService, times(0)).updateLatestCheckpoint(eq(ctx), any());
        verify(executionLogService, times(1))
                .logCompleted(eq(ctx), eq(1L), eq(0L), eq(0L), eq(0L), eq(1L), eq(1L), eq(1L), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"POSITION", "POSITION_TOKENS", "POSITION_TRANSFERS", "EXTRA_INFO", "EVENTS_WF"})
    void shouldForwardFailedRowsToStagingForEachEntity(String entityName) throws Exception {
        EntityName entity = EntityName.valueOf(entityName);
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext(entityName, "run-stage-all-" + entityName, runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(5));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(entity)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", checkpoint.plusSeconds(30).toString());
        row.put("NAV", "NAV-1");
        HashMap<String, Object> rows = new HashMap<>();
        rows.put("row-1", row);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));
        when(entityTransformer.transform(eq(row), any(), eq(ctx), eq(entity)))
                .thenThrow(new EntityTransformer.TransformationException("Synthetic transform error"));
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(entity), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(0, 1, checkpoint.plusSeconds(30)));

        runner.runEntity(ctx);

        ArgumentCaptor<List> payloadCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List> stagingCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Instant> checkpointCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(windowCyclePersistenceService, times(1))
                .persistWindowCycle(eq(ctx), eq(entity), payloadCaptor.capture(), stagingCaptor.capture(), any(),
                        checkpointCaptor.capture());
        assertEquals(0, payloadCaptor.getValue().size());
        assertEquals(1, stagingCaptor.getValue().size());
        // All-staged window (0 inserted): the checkpoint offered to persistence must still advance
        // past the window start, so the window is not re-read from ADX on the next run.
        assertTrue(checkpointCaptor.getValue().isAfter(checkpoint),
                "all-staged window must advance the checkpoint beyond the cursor, was " + checkpointCaptor.getValue());
        // A liveness/progress heartbeat is written for the processed window.
        verify(executionLogService, times(1))
                .heartbeat(eq(ctx), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong());
    }

    @Test
    void shouldPersistCurrentWindowBeforeGuardrailStopsNextIteration() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("POSITION", "run-persist", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(10));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", checkpoint.plusSeconds(15).toString());
        HashMap<String, Object> rows = new HashMap<>();
        rows.put("row-1", row);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        Position transformed = new Position();
        transformed.setDateEvent(checkpoint.atZone(ZoneOffset.UTC).toLocalDate());
        when(entityTransformer.transform(eq(row), eq(Position.class), eq(ctx), eq(EntityName.POSITION)))
                .thenReturn(transformed);

        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, checkpoint.plusSeconds(15)));

        runner.runEntity(ctx);

        verify(windowCyclePersistenceService, times(1))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any());
        verify(executionLogService, times(1)).updateLatestCheckpoint(eq(ctx), any());
        verify(executionLogService, times(1))
                .logCompleted(eq(ctx), eq(1L), eq(1L), eq(1L), eq(0L), eq(0L), eq(1L), eq(1L), any());
    }

    @Test
    void shouldRetryWindowPersistOnTransientDeadlockThenSucceed() throws Exception {
        ingestionConfig.getPersistence().getRetry().setInitialBackoff(Duration.ofMillis(1));
        ingestionConfig.getPersistence().getRetry().setMaxBackoff(Duration.ofMillis(1));

        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("POSITION", "run-retry", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(10));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", checkpoint.plusSeconds(15).toString());
        HashMap<String, Object> rows = new HashMap<>();
        rows.put("row-1", row);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        Position transformed = new Position();
        transformed.setDateEvent(checkpoint.atZone(ZoneOffset.UTC).toLocalDate());
        when(entityTransformer.transform(eq(row), eq(Position.class), eq(ctx), eq(EntityName.POSITION)))
                .thenReturn(transformed);

        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any()))
                .thenThrow(new CannotAcquireLockException("deadlock detected"))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, checkpoint.plusSeconds(15)));

        runner.runEntity(ctx);

        verify(windowCyclePersistenceService, times(2))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any());
        verify(executionLogService, times(1)).updateLatestCheckpoint(eq(ctx), any());
        verify(executionLogService, times(1))
                .logCompleted(eq(ctx), eq(1L), eq(1L), eq(1L), eq(0L), eq(0L), eq(1L), eq(1L), any());
    }

    @Test
    void shouldNotResendStagingRecordsWhenRetryingWindowPersist() throws Exception {
        ingestionConfig.getPersistence().getRetry().setInitialBackoff(Duration.ofMillis(1));
        ingestionConfig.getPersistence().getRetry().setMaxBackoff(Duration.ofMillis(1));

        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("POSITION", "run-retry-staging", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(10));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> okRow = new HashMap<>();
        okRow.put("INSERTED_TIMESTAMP", checkpoint.plusSeconds(15).toString());
        Map<String, Object> failRow = new HashMap<>();
        failRow.put("INSERTED_TIMESTAMP", checkpoint.plusSeconds(10).toString());
        HashMap<String, Object> rows = new HashMap<>();
        rows.put("row-ok", okRow);
        rows.put("row-fail", failRow);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                2,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        Position transformed = new Position();
        transformed.setDateEvent(checkpoint.atZone(ZoneOffset.UTC).toLocalDate());
        when(entityTransformer.transform(eq(okRow), eq(Position.class), eq(ctx), eq(EntityName.POSITION)))
                .thenReturn(transformed);
        when(entityTransformer.transform(eq(failRow), eq(Position.class), eq(ctx), eq(EntityName.POSITION)))
                .thenThrow(new EntityTransformer.TransformationException("Synthetic transform error"));

        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any()))
                .thenThrow(new CannotAcquireLockException("deadlock detected"))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 1, checkpoint.plusSeconds(15)));

        runner.runEntity(ctx);

        ArgumentCaptor<List> stagingCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List> discardedCaptor = ArgumentCaptor.forClass(List.class);
        verify(windowCyclePersistenceService, times(2))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(),
                        stagingCaptor.capture(), discardedCaptor.capture(), any());

        assertEquals(1, stagingCaptor.getAllValues().get(0).size());
        assertEquals(0, stagingCaptor.getAllValues().get(1).size());
        assertEquals(0, discardedCaptor.getAllValues().get(1).size());
    }

    @Test
    void shouldConsolidatePositionsResolvedToSyntheticBatchId() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("POSITION", "run-consolidate-position", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = runStart;
        Instant firstTimestamp = checkpoint.plusSeconds(30);
        Instant secondTimestamp = checkpoint.plusSeconds(60);

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> firstRow = Map.of("INSERTED_TIMESTAMP", firstTimestamp.toString());
        Map<String, Object> secondRow = Map.of("INSERTED_TIMESTAMP", secondTimestamp.toString());
        Map<String, Object> rows = new HashMap<>();
        rows.put("first", firstRow);
        rows.put("second", secondRow);
        AdxWindowResult window = new AdxWindowResult(
                checkpoint, endLimit, Duration.ofMinutes(5), 1, rows);
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        Position firstPosition = Position.builder()
                .dateEvent(LocalDate.of(2026, 7, 17))
                .insertedTimestamp(firstTimestamp.atZone(ZoneOffset.UTC).toLocalDateTime())
                .lastEvent(firstTimestamp.atZone(ZoneOffset.UTC).toLocalDateTime())
                .nav("NAV-1")
                .paEmittente("PA-1")
                .dateEvents("[]")
                .build();
        Position duplicatePosition = Position.builder()
                .id(-1)
                .dateEvent(LocalDate.of(2026, 7, 17))
                .insertedTimestamp(secondTimestamp.atZone(ZoneOffset.UTC).toLocalDateTime())
                .lastEvent(secondTimestamp.atZone(ZoneOffset.UTC).toLocalDateTime())
                .nav("NAV-1")
                .paEmittente("PA-1")
                .dateEvents("[]")
                .build();
        when(entityTransformer.transform(eq(firstRow), eq(Position.class), eq(ctx), eq(EntityName.POSITION)))
                .thenReturn(firstPosition);
        when(entityTransformer.transform(eq(secondRow), eq(Position.class), eq(ctx), eq(EntityName.POSITION)))
                .thenReturn(duplicatePosition);
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, secondTimestamp));

        runner.runEntity(ctx);

        ArgumentCaptor<List> payloadCaptor = ArgumentCaptor.forClass(List.class);
        verify(windowCyclePersistenceService).persistWindowCycle(
                eq(ctx), eq(EntityName.POSITION), payloadCaptor.capture(), any(), any(), eq(secondTimestamp));
        assertEquals(1, payloadCaptor.getValue().size());
        Position persistedPosition = (Position) payloadCaptor.getValue().get(0);
        assertEquals(null, persistedPosition.getId());
        assertEquals(secondTimestamp.atZone(ZoneOffset.UTC).toLocalDateTime(), persistedPosition.getLastEvent());
        assertEquals(1, ctx.getRecordsConsolidated());
        verify(executionLogService).logCompleted(
                eq(ctx), eq(2L), eq(2L), eq(1L), eq(0L), eq(0L), eq(1L), eq(1L), any());
    }

    @Test
    void shouldUseConfiguredPerEntityWindow() {
        Instant runStart = Instant.parse("2026-05-08T12:00:00Z");
        RunContext ctx = new RunContext("EXTRA_INFO", "run-extra-window", runStart);
        Instant checkpoint = Instant.parse("2026-05-08T09:00:00Z");
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(30));

        ingestionConfig.getAdx().setWindows(Map.of(EntityName.EXTRA_INFO, Duration.ofMinutes(30)));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EXTRA_INFO)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                checkpoint,
                endLimit,
                Duration.ofMinutes(30),
                1,
                new HashMap<>()
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(30)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));

        runner.runEntity(ctx);

        verify(adxQueryService, times(1))
                .fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(30)), eq(endLimit));
    }

    @Test
    void shouldUseCatchUpWindowForEventsWfWhenLagIsHigh() {
        Instant runStart = Instant.parse("2026-07-14T10:00:00Z");
        RunContext ctx = new RunContext("EVENTS_WF", "run-events-catchup", runStart);
        Instant checkpoint = Instant.parse("2026-07-14T07:00:00Z");
        Instant endLimit = Instant.parse("2026-07-14T10:00:00Z");

        ingestionConfig.getAdx().setWindows(Map.of(EntityName.EVENTS_WF, Duration.ofMinutes(5)));
        ingestionConfig.getEventsWf().getCatchup().setEnabled(true);
        ingestionConfig.getEventsWf().getCatchup().setLagThreshold(Duration.ofHours(1));
        ingestionConfig.getEventsWf().getCatchup().setWindow(Duration.ofMinutes(20));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EVENTS_WF)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(20)),
                Duration.ofMinutes(20),
                1,
                new HashMap<>()
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(20)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));

        runner.runEntity(ctx);

        verify(adxQueryService, times(1))
                .fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(20)), eq(endLimit));
    }

    @Test
    void shouldUseRealtimeWindowForEventsWfWhenLagIsLow() {
        Instant runStart = Instant.parse("2026-07-14T10:00:00Z");
        RunContext ctx = new RunContext("EVENTS_WF", "run-events-realtime", runStart);
        Instant checkpoint = Instant.parse("2026-07-14T09:00:00Z");
        Instant endLimit = Instant.parse("2026-07-14T10:00:00Z");

        ingestionConfig.getAdx().setWindows(Map.of(EntityName.EVENTS_WF, Duration.ofMinutes(5)));
        ingestionConfig.getEventsWf().getCatchup().setEnabled(true);
        ingestionConfig.getEventsWf().getCatchup().setLagThreshold(Duration.ofHours(4));
        ingestionConfig.getEventsWf().getCatchup().setWindow(Duration.ofMinutes(20));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EVENTS_WF)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                new HashMap<>()
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));

        runner.runEntity(ctx);

        verify(adxQueryService, times(1))
                .fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit));
    }

    @Test
    void shouldDiscardExtraInfoNotInWhitelistBeforePersistence() throws Exception {
        Instant runStart = Instant.parse("2026-06-17T09:00:00Z");
        RunContext ctx = new RunContext("EXTRA_INFO", "run-extra-sensitive", runStart);
        Instant checkpoint = Instant.parse("2026-06-17T08:55:00Z");
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(5));

        when(extraInfoWhitelistService.isAllowed("email")).thenReturn(false);
        ingestionConfig.getGuardrails().setEnableMaxDuration(false);

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EXTRA_INFO)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> row = new HashMap<>();
        row.put("INSERTED_TIMESTAMP", checkpoint.plusSeconds(30).toString());
        row.put("INFO_NAME", "email");
        HashMap<String, Object> rows = new HashMap<>();
        rows.put("row-1", row);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                endLimit,
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        ExtraInfo transformed = new ExtraInfo();
        transformed.setInfoName("email");
        when(entityTransformer.transform(eq(row), eq(ExtraInfo.class), eq(ctx), eq(EntityName.EXTRA_INFO)))
                .thenReturn(transformed);
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.EXTRA_INFO), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(0, 0, checkpoint.plusSeconds(30)));

        runner.runEntity(ctx);

        verify(entityTransformer, times(1)).transform(eq(row), eq(ExtraInfo.class), eq(ctx), eq(EntityName.EXTRA_INFO));
        ArgumentCaptor<List> discardedCaptor = ArgumentCaptor.forClass(List.class);
        verify(windowCyclePersistenceService, times(1))
                .persistWindowCycle(eq(ctx), eq(EntityName.EXTRA_INFO), any(), any(), discardedCaptor.capture(), any());
        assertEquals(1, discardedCaptor.getValue().size());
        verify(executionLogService, times(1))
                .logCompleted(eq(ctx), eq(1L), eq(0L), eq(0L), eq(1L), eq(0L), eq(1L), eq(1L), any());
    }

    @Test
    void shouldPrefetchEventsPositionOnlyForRowsWithoutResolvedToken() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("EVENTS_WF", "run-events-prefetch-selective", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(5));
        ingestionConfig.getGuardrails().setEnableMaxDuration(false);

        ingestionConfig.getEventsWf().getPrefetch().setEnabled(true);
        ingestionConfig.getEventsWf().getPrefetch().setMinDistinctTokenKeys(1);
        ingestionConfig.getEventsWf().getPrefetch().setMinDistinctPositionKeys(1);

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EVENTS_WF)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> rowWithToken = new HashMap<>();
        rowWithToken.put("INSERTED_TIMESTAMP_RESP", checkpoint.plusSeconds(30).toString());
        rowWithToken.put("TOKEN", "token-hit");
        rowWithToken.put("NAV", "NAV-1");
        rowWithToken.put("PA_EMITTENTE", "PA-1");

        Map<String, Object> rowWithoutToken = new HashMap<>();
        rowWithoutToken.put("INSERTED_TIMESTAMP_RESP", checkpoint.plusSeconds(40).toString());
        rowWithoutToken.put("NAV", "NAV-2");
        rowWithoutToken.put("PA_EMITTENTE", "PA-2");

        Map<String, Object> rows = new LinkedHashMap<>();
        rows.put("with-token", rowWithToken);
        rows.put("without-token", rowWithoutToken);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        when(entityTransformer.transform(any(), eq(EventsWf.class), eq(ctx), eq(EntityName.EVENTS_WF)))
                .thenReturn(new EventsWf());
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.EVENTS_WF), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(2, 0, checkpoint.plusSeconds(40)));

        String tokenBase64 = java.util.Base64.getEncoder().encodeToString("token-hit".getBytes());
        org.mockito.Mockito.doAnswer(invocation -> {
            BatchLocalCache cache = invocation.getArgument(1);
            cache.putTokenWindowPrefetch(tokenBase64, 123);
            cache.cacheTokenCanonicalLookupResult(tokenBase64, 123);
            cache.cacheTokenCanonicalFkPosition(tokenBase64, 321);
            return null;
        }).when(tokenFkBatchPrefetcher).prefetchForPositionTransfers(eq(rows), any(), eq(ctx));

        runner.runEntity(ctx);

        ArgumentCaptor<Map<String, Object>> positionRowsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(positionFkBatchPrefetcher).prefetchForPositionTokens(positionRowsCaptor.capture(), any(), eq(ctx));
        assertEquals(1, positionRowsCaptor.getValue().size());
        assertEquals(rowWithoutToken, positionRowsCaptor.getValue().get("without-token"));
    }

    @Test
    void shouldDeferGraceZoneRowsAndCapCheckpointAtWindowEndForEventsWf() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("EVENTS_WF", "run-events-grace-defer", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant windowTo = checkpoint.plus(Duration.ofMinutes(5));
        Instant endLimit = windowTo;
        ingestionConfig.getGuardrails().setEnableMaxDuration(false);

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EVENTS_WF)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> inWindowRow = new HashMap<>();
        inWindowRow.put("INSERTED_TIMESTAMP_REQ", checkpoint.plusSeconds(120).toString());
        inWindowRow.put("TOKEN", "token-in-window");

        Map<String, Object> graceRow = new HashMap<>();
        graceRow.put("INSERTED_TIMESTAMP_REQ", windowTo.plusSeconds(60).toString());
        graceRow.put("TOKEN", "token-grace-zone");

        Map<String, Object> rows = new LinkedHashMap<>();
        rows.put("in-window", inWindowRow);
        rows.put("grace-zone", graceRow);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                windowTo,
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        when(entityTransformer.transform(any(), eq(EventsWf.class), eq(ctx), eq(EntityName.EVENTS_WF)))
                .thenReturn(new EventsWf());
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.EVENTS_WF), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, windowTo));

        runner.runEntity(ctx);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Object>> payloadCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Instant> checkpointCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(windowCyclePersistenceService).persistWindowCycle(eq(ctx), eq(EntityName.EVENTS_WF),
                payloadCaptor.capture(), any(), any(), checkpointCaptor.capture());

        // grace-zone row (anchor >= window end) is deferred to the next window
        assertEquals(1, payloadCaptor.getValue().size());
        // checkpoint is capped at window end, never overshooting into the +grace zone
        assertEquals(windowTo, checkpointCaptor.getValue());
        // transform is invoked only for the in-window row, not the deferred one
        verify(entityTransformer, times(1))
                .transform(any(), eq(EventsWf.class), eq(ctx), eq(EntityName.EVENTS_WF));
    }

    @Test
    void shouldNotOvershootParentWhenEventsWfWindowIsAllStagedWithGraceZoneRow() throws Exception {
        // Combines the grace-zone deferral with the checkpoint-advance-on-staging change: even when
        // the in-window event fails its FK (0 inserted, all staged), the checkpoint must still be
        // capped at the window end (= the POSITION_TOKENS parent checkpoint / endLimit) and the
        // grace-zone row deferred — so EVENTS_WF can never advance past its parent via the +grace
        // read-ahead. Guards the user-reported concern.
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("EVENTS_WF", "run-events-grace-stage", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant windowTo = checkpoint.plus(Duration.ofMinutes(5));
        Instant endLimit = windowTo; // endLimit = parent (POSITION_TOKENS) checkpoint
        ingestionConfig.getGuardrails().setEnableMaxDuration(false);

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EVENTS_WF)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> inWindowRow = new HashMap<>();
        inWindowRow.put("INSERTED_TIMESTAMP_REQ", checkpoint.plusSeconds(120).toString());
        inWindowRow.put("TOKEN", "token-in-window");
        Map<String, Object> graceRow = new HashMap<>();
        graceRow.put("INSERTED_TIMESTAMP_REQ", windowTo.plusSeconds(60).toString());
        graceRow.put("TOKEN", "token-grace-zone");
        Map<String, Object> rows = new LinkedHashMap<>();
        rows.put("in-window", inWindowRow);
        rows.put("grace-zone", graceRow);

        AdxWindowResult window = new AdxWindowResult(checkpoint, windowTo, Duration.ofMinutes(5), 1, rows);
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));
        // in-window event fails its FK -> staged; 0 inserted
        when(entityTransformer.transform(any(), eq(EventsWf.class), eq(ctx), eq(EntityName.EVENTS_WF)))
                .thenThrow(new MissingForeignKeyException("Missing required FK fkTokens"));
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.EVENTS_WF), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(0, 1, windowTo));

        runner.runEntity(ctx);

        ArgumentCaptor<List> stagingCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<Instant> checkpointCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(windowCyclePersistenceService).persistWindowCycle(eq(ctx), eq(EntityName.EVENTS_WF),
                any(), stagingCaptor.capture(), any(), checkpointCaptor.capture());
        // only the in-window event is staged; the grace-zone row is deferred (transform called once)
        assertEquals(1, stagingCaptor.getValue().size());
        verify(entityTransformer, times(1))
                .transform(any(), eq(EventsWf.class), eq(ctx), eq(EntityName.EVENTS_WF));
        // even all-staged, the checkpoint is capped at the window end (= parent checkpoint), not +grace
        assertEquals(windowTo, checkpointCaptor.getValue());
    }

    @Test
    void shouldSkipEventsPrefetchWhenDistinctKeysAreBelowThreshold() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("EVENTS_WF", "run-events-prefetch-threshold", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(5));
        ingestionConfig.getGuardrails().setEnableMaxDuration(false);

        ingestionConfig.getEventsWf().getPrefetch().setEnabled(true);
        ingestionConfig.getEventsWf().getPrefetch().setMinDistinctTokenKeys(10);
        ingestionConfig.getEventsWf().getPrefetch().setMinDistinctPositionKeys(10);

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.EVENTS_WF)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        Map<String, Object> rowWithToken = new HashMap<>();
        rowWithToken.put("INSERTED_TIMESTAMP_RESP", checkpoint.plusSeconds(30).toString());
        rowWithToken.put("TOKEN", "token-1");
        rowWithToken.put("NAV", "NAV-1");
        rowWithToken.put("PA_EMITTENTE", "PA-1");

        Map<String, Object> rows = new LinkedHashMap<>();
        rows.put("with-token", rowWithToken);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                checkpoint.plus(Duration.ofMinutes(5)),
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        when(entityTransformer.transform(any(), eq(EventsWf.class), eq(ctx), eq(EntityName.EVENTS_WF)))
                .thenReturn(new EventsWf());
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.EVENTS_WF), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, checkpoint.plusSeconds(30)));

        runner.runEntity(ctx);

        verify(tokenFkBatchPrefetcher, never()).prefetchForPositionTransfers(any(), any(), eq(ctx));
        verify(positionFkBatchPrefetcher, never()).prefetchForPositionTokens(any(), any(), eq(ctx));
    }

    @Test
    void shouldPersistEffectiveRunWindowWhenGuardrailStopsBeforeEndLimit() throws Exception {
        // Exercises the legacy step-advance path (empty-window probe disabled): a guardrail stop must
        // persist the partial run window actually covered, not the full endLimit.
        ingestionConfig.getAdx().setEmptyWindowProbeEnabled(false);
        Instant runStart = Instant.parse("2026-05-13T16:46:11Z");
        RunContext ctx = new RunContext("POSITION", "run-window-effective", runStart);
        Instant checkpoint = Instant.parse("2026-03-23T01:33:31.645Z");
        Instant endLimit = Instant.parse("2026-05-13T16:46:11.733Z");
        Instant expectedTo = checkpoint.plus(Duration.ofMinutes(5));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                checkpoint,
                expectedTo,
                Duration.ofMinutes(5),
                1,
                new HashMap<>()
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(emptyWindow));
        runner.runEntity(ctx);

        verify(executionLogService, times(1)).updateRunWindow(eq(ctx), eq(checkpoint), eq(expectedTo));
        verify(windowCyclePersistenceService, times(0))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any());
    }

    @Test
    void shouldCheckpointLastTransformedRecordWhenGuardrailInterruptsTransformMidWindow() throws Exception {
        Instant runStart = Instant.now();
        RunContext ctx = new RunContext("POSITION", "run-guardrail-mid-transform", runStart);
        Instant checkpoint = runStart.minus(Duration.ofMinutes(5));
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(5));
        Instant firstTs = checkpoint.plusSeconds(10);
        Instant secondTs = checkpoint.plusSeconds(50);

        ingestionConfig.getGuardrails().setEnableMaxDuration(true);
        ingestionConfig.getGuardrails().setMaxDuration(Duration.ofMillis(100));

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(EntityName.POSITION)).thenReturn(Optional.of(checkpoint));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, true);

        Map<String, Object> row1 = new HashMap<>();
        row1.put("INSERTED_TIMESTAMP", firstTs.toString());
        Map<String, Object> row2 = new HashMap<>();
        row2.put("INSERTED_TIMESTAMP", secondTs.toString());
        HashMap<String, Object> rows = new HashMap<>();
        rows.put("row-1", row1);
        rows.put("row-2", row2);

        AdxWindowResult window = new AdxWindowResult(
                checkpoint,
                endLimit,
                Duration.ofMinutes(5),
                1,
                rows
        );
        when(adxQueryService.fetchWindow(eq(ctx), eq(checkpoint), eq(Duration.ofMinutes(5)), eq(endLimit)))
                .thenReturn(Optional.of(window));

        AtomicInteger callCount = new AtomicInteger();
        when(entityTransformer.transform(any(), eq(Position.class), eq(ctx), eq(EntityName.POSITION)))
                .thenAnswer(invocation -> {
                    if (callCount.incrementAndGet() == 1) {
                        Thread.sleep(150);
                    }
                    Position transformed = new Position();
                    transformed.setDateEvent(LocalDate.now());
                    return transformed;
                });

        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, firstTs));

        runner.runEntity(ctx);

        verify(windowCyclePersistenceService, atLeastOnce())
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any(), eq(firstTs));
        verify(executionLogService, times(1)).updateLatestCheckpoint(eq(ctx), eq(firstTs));
    }
}
