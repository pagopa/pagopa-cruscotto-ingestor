package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.CheckpointStoreService;
import it.pagopa.cruscotto.ingestion.service.EndLimitResolverService;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.service.RunGuardrails;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryService;
import it.pagopa.cruscotto.ingestion.service.adx.AdxWindowResult;
import it.pagopa.cruscotto.ingestion.entity.Position;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
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

    private GenericIngestionRunnerImpl runner;

    private IngestionConfig ingestionConfig;

    @BeforeEach
    void setUp() {
        ingestionConfig = new IngestionConfig();
        ingestionConfig.setInitialWindow(Duration.ofMinutes(5));
        ingestionConfig.setFirstRunLookback(Period.ofMonths(6));

        runner = new GenericIngestionRunnerImpl(
                checkpointStore,
                endLimitResolver,
                runGuardrails,
                adxQueryService,
                oldestTimestampProvider,
                executionLogService,
                windowCyclePersistenceService,
                ingestionConfig,
                entityTransformer
        );
    }

    @Test
    void shouldStartFromLookbackFloorWhenCheckpointAndAdxOldestAreMissing() {
        Instant runStart = Instant.parse("2026-05-08T12:00:00Z");
        RunContext ctx = new RunContext("POSITION", "run-123", runStart);
        Instant endLimit = Instant.parse("2026-05-08T10:00:00Z");
        Instant expectedCursor = runStart.atZone(ZoneOffset.UTC).minus(Period.ofMonths(6)).toInstant();

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
    void shouldStartFromAdxOldestWhenItIsNewerThanLookbackFloor() {
        Instant runStart = Instant.parse("2026-05-08T12:00:00Z");
        RunContext ctx = new RunContext("POSITION", "run-456", runStart);
        Instant endLimit = Instant.parse("2026-05-08T10:00:00Z");
        Instant adxOldest = Instant.parse("2026-04-08T12:00:00Z");

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
    void shouldClampToLookbackFloorWhenAdxOldestIsOlderThanSixMonths() {
        Instant runStart = Instant.parse("2026-05-08T12:00:00Z");
        RunContext ctx = new RunContext("POSITION", "run-789", runStart);
        Instant endLimit = Instant.parse("2026-05-08T10:00:00Z");
        Instant lookbackFloor = runStart.atZone(ZoneOffset.UTC).minus(Period.ofMonths(6)).toInstant();
        Instant adxOldest = Instant.parse("2025-01-01T00:00:00Z");

        when(endLimitResolver.resolveEndLimit(ctx)).thenReturn(Optional.of(endLimit));
        when(checkpointStore.getCheckpoint(any())).thenReturn(Optional.empty());
        when(oldestTimestampProvider.getOldestTimestamp(ctx, EntityName.POSITION))
                .thenReturn(Optional.of(adxOldest));
        when(runGuardrails.ok(eq(ctx), anyLong(), anyLong())).thenReturn(true, false);

        AdxWindowResult emptyWindow = new AdxWindowResult(
                lookbackFloor,
                lookbackFloor.plus(Duration.ofMinutes(5)),
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
        assertEquals(lookbackFloor, cursorCaptor.getValue());
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
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION_TOKENS), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(0, 1, checkpoint.plusSeconds(30)));

        runner.runEntity(ctx);

        verify(windowCyclePersistenceService, times(1))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION_TOKENS), any(), any(), any());
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
        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(entity), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(0, 1, checkpoint.plusSeconds(30)));

        runner.runEntity(ctx);

        ArgumentCaptor<List> payloadCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List> stagingCaptor = ArgumentCaptor.forClass(List.class);
        verify(windowCyclePersistenceService, times(1))
                .persistWindowCycle(eq(ctx), eq(entity), payloadCaptor.capture(), stagingCaptor.capture(), any());
        assertEquals(0, payloadCaptor.getValue().size());
        assertEquals(1, stagingCaptor.getValue().size());
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

        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, checkpoint.plusSeconds(15)));

        runner.runEntity(ctx);

        verify(windowCyclePersistenceService, times(1))
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any());
        verify(executionLogService, times(1)).updateLatestCheckpoint(eq(ctx), any());
        verify(executionLogService, times(1))
                .logCompleted(eq(ctx), eq(1L), eq(1L), eq(1L), eq(0L), eq(0L), eq(1L), eq(1L), any());
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
    void shouldDiscardSensitiveExtraInfoByBlacklistBeforePersistence() throws Exception {
        Instant runStart = Instant.parse("2026-06-17T09:00:00Z");
        RunContext ctx = new RunContext("EXTRA_INFO", "run-extra-sensitive", runStart);
        Instant checkpoint = Instant.parse("2026-06-17T08:55:00Z");
        Instant endLimit = checkpoint.plus(Duration.ofMinutes(5));

        ingestionConfig.getExtraInfo().setInfoNameBlacklist(List.of("email"));
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

        runner.runEntity(ctx);

        verify(entityTransformer, times(1)).transform(eq(row), eq(ExtraInfo.class), eq(ctx), eq(EntityName.EXTRA_INFO));
        verify(windowCyclePersistenceService, never())
                .persistWindowCycle(eq(ctx), eq(EntityName.EXTRA_INFO), any(), any(), any());
        verify(executionLogService, times(1))
                .logCompleted(eq(ctx), eq(1L), eq(0L), eq(0L), eq(1L), eq(0L), eq(1L), eq(1L), any());
    }

    @Test
    void shouldPersistEffectiveRunWindowWhenGuardrailStopsBeforeEndLimit() throws Exception {
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
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any());
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

        when(windowCyclePersistenceService.persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), any()))
                .thenReturn(new WindowCyclePersistenceService.WindowCycleResult(1, 0, firstTs));

        runner.runEntity(ctx);

        verify(windowCyclePersistenceService, atLeastOnce())
                .persistWindowCycle(eq(ctx), eq(EntityName.POSITION), any(), any(), eq(firstTs));
        verify(executionLogService, times(1)).updateLatestCheckpoint(eq(ctx), eq(firstTs));
    }
}



