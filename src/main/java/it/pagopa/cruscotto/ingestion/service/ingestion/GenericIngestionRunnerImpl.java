package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.batch.GenericIngestionRunner;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.service.CheckpointStoreService;
import it.pagopa.cruscotto.ingestion.service.EndLimitResolverService;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.service.ExtraInfoWhitelistService;
import it.pagopa.cruscotto.ingestion.service.RunGuardrails;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryService;
import it.pagopa.cruscotto.ingestion.service.adx.AdxWindowResult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class GenericIngestionRunnerImpl implements GenericIngestionRunner {

    private static final String END_REASON_COMPLETED = "COMPLETED";
    private static final String END_REASON_GUARDRAIL_MAX_DURATION = "GUARDRAIL_MAX_DURATION";
    private static final String END_REASON_GUARDRAIL_MAX_QUERIES = "GUARDRAIL_MAX_QUERIES";
    private static final String END_REASON_GUARDRAIL_MAX_ROWS = "GUARDRAIL_MAX_ROWS";
    private static final String END_REASON_GUARDRAIL_LIMIT = "GUARDRAIL_LIMIT";
    private static final String END_REASON_NOOP_CURSOR_AT_END_LIMIT = "NOOP_CURSOR_AT_END_LIMIT";
    private static final String END_REASON_NOOP_CHILD_OLDEST_AFTER_PARENT_CHECKPOINT = "NOOP_CHILD_OLDEST_AFTER_PARENT_CHECKPOINT";
    private static final String WINDOW_PROFILE_CATCH_UP = "CATCH_UP";
    private static final String WINDOW_PROFILE_REALTIME = "REALTIME";
    private static final String WINDOW_PROFILE_STANDARD = "STANDARD";
    private static final String DISCARD_REASON_EXTRA_INFO_NOT_WHITELIST = "EXTRA_INFO not in whitelist";

    private final CheckpointStoreService checkpointStore;
    private final EndLimitResolverService endLimitResolver;
    private final RunGuardrails runGuardrails;
    private final AdxQueryService adxQueryService;
    private final OldestTimestampProvider oldestTimestampProvider;
    private final ExecutionLogService executionLogService;
    private final WindowCyclePersistenceService windowCyclePersistenceService;
    private final IngestionConfig ingestionConfig;
    private final EntityTransformer entityTransformer;
    private final ExtraInfoWhitelistService extraInfoWhitelistService;
    private final PositionFkBatchPrefetcher positionFkBatchPrefetcher;
    private final TokenFkBatchPrefetcher tokenFkBatchPrefetcher;

    @Override
    public void runEntity(RunContext ctx) {
        EntityName entity = EntityName.valueOf(ctx.getEntityName());

        LogHelper.info(ctx, RunPhase.START, "");
        
        // Log execution start
        executionLogService.logStarted(ctx, "batch-" + entity.name());

        long queriesExecuted = 0;
        long operationCount = 0;
        long rowsProcessed = 0;
        long recordsRead = 0;
        long recordsTransformed = 0;
        long recordsInserted = 0;
        long recordsDiscarded = 0;
        long recordsStaged = 0;
        String endReason = END_REASON_COMPLETED;
        Instant runWindowFromTs = null;
        Instant runWindowToTs = null;

        try {
            // 1. Resolve endLimit
            EndLimitResolverService.EndLimitResolution endLimitResolution = endLimitResolver.resolveEndLimitDetailed(ctx);
            if (endLimitResolution == null) {
                Optional<Instant> fallbackEndLimit = endLimitResolver.resolveEndLimit(ctx);
                endLimitResolution = new EndLimitResolverService.EndLimitResolution(
                        fallbackEndLimit,
                        fallbackEndLimit.isPresent() ? EndLimitResolverService.REASON_RESOLVED : "NO_END_LIMIT"
                );
            }
            Optional<Instant> endLimitOpt = endLimitResolution.endLimit();
            if (endLimitOpt.isEmpty()) {
                endReason = endLimitResolution.reason();
                LogHelper.info(ctx, RunPhase.NOOP, "endLimit not resolved, reason=" + endReason);
            } else {
                Instant endLimit = endLimitOpt.orElseThrow(
                        () -> new IllegalStateException("End limit unexpectedly absent"));

                Instant cursor = determineCursor(ctx, entity);

                // Guard: if the cursor is already at or past the endLimit there is nothing to do
                // this run (e.g. parent entity hasn't caught up to the child's ADX oldest yet).
                if (!cursor.isBefore(endLimit)) {
                    Optional<FirstRunParentGap> firstRunParentGap = resolveFirstRunParentGap(ctx, entity);
                    if (firstRunParentGap.isPresent()) {
                        FirstRunParentGap gap = firstRunParentGap.orElseThrow(
                                () -> new IllegalStateException("First-run parent gap unexpectedly absent"));
                        endReason = END_REASON_NOOP_CHILD_OLDEST_AFTER_PARENT_CHECKPOINT;
                        LogHelper.info(ctx, RunPhase.NOOP,
                                "first-run skipped because child oldest ADX timestamp is after parent checkpoint:"
                                        + " childOldestAdx=" + gap.childOldestAdx()
                                        + ", parentEntity=" + gap.parentEntity().name()
                                        + ", parentCheckpoint=" + gap.parentCheckpoint()
                                        + ", endReason=" + END_REASON_NOOP_CHILD_OLDEST_AFTER_PARENT_CHECKPOINT);
                    } else {
                        endReason = END_REASON_NOOP_CURSOR_AT_END_LIMIT;
                        LogHelper.info(ctx, RunPhase.NOOP,
                                "cursor is already at or past endLimit; cursor=" + cursor
                                        + ", endLimit=" + endLimit
                                        + " — no rows will be processed this run");
                    }
                } else {
                    runWindowFromTs = cursor;
                    runWindowToTs = cursor;
                    String windowProfile = null;

                    // Record initial_ts BEFORE processing starts.
                    // Idempotent: the SQL COALESCE ensures it is never overwritten on subsequent runs.
                    checkpointStore.initializeInitialTs(entity, cursor, ctx.getRunId());

                    while (cursor.isBefore(endLimit)) {
                        Duration configuredWindow = ingestionConfig.resolveWindowForRun(entity, cursor, endLimit);
                        String currentWindowProfile = resolveWindowProfile(entity, configuredWindow);
                        ctx.setCatchupMode(WINDOW_PROFILE_CATCH_UP.equals(currentWindowProfile));
                        if (!Objects.equals(windowProfile, currentWindowProfile)) {
                            LogHelper.info(ctx, RunPhase.WINDOW,
                                    "windowProfile=" + currentWindowProfile + ", window=" + configuredWindow + ", cursor=" + cursor + ", endLimit=" + endLimit);
                            windowProfile = currentWindowProfile;
                        }

                        if (!runGuardrails.ok(ctx, queriesExecuted, rowsProcessed)) {
                            endReason = resolveGuardrailEndReason(ctx, queriesExecuted, rowsProcessed);
                            LogHelper.warn(ctx, RunPhase.SKIP, "Guardrail stop detected, ending run with reason=" + endReason);
                            break;
                        }

                        ctx.setOperationId(UUID.randomUUID().toString());
                        operationCount++;
                        long adxQueryStartNs = System.nanoTime();
                        Optional<AdxWindowResult> windowOpt = adxQueryService.fetchWindow(
                                ctx,
                                cursor,
                                configuredWindow,
                                endLimit
                        );
                        ctx.setAdxQueryDurationMs(ctx.getAdxQueryDurationMs()
                                + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - adxQueryStartNs));

                        if (windowOpt.isEmpty()) {
                            LogHelper.warn(ctx, RunPhase.WINDOW, "No result returned");
                            break;
                        }

                        AdxWindowResult window = windowOpt.orElseThrow(
                                () -> new IllegalStateException("ADX window unexpectedly absent"));
                        ctx.incrementAdxWindowCount();
                        ctx.addAdxAttemptCount(window.getAttempts());
                        queriesExecuted += window.getAttempts();
                        int extractedRows = window.getRows() != null ? window.getRows().size() : 0;
                        recordsRead += extractedRows;
                        LogHelper.info(ctx, RunPhase.WINDOW,
                                "window extractedRows=" + extractedRows
                                        + ", from=" + window.getFromInclusive()
                                        + ", to=" + window.getToExclusive()
                                        + ", attempts=" + window.getAttempts());

                        if (window.getRows() == null || window.getRows().isEmpty()) {
                            ctx.incrementEmptyWindowCount();
                            cursor = min(cursor.plus(window.getWindowUsed()), endLimit);
                            runWindowToTs = cursor;
                            LogHelper.info(ctx, RunPhase.WINDOW, "empty rows, queriesExecuted=" + queriesExecuted + ", rowsProcessed=" + rowsProcessed + ", cursor=" + cursor);
                            continue;
                        }

                        long ingestorLogicStartNs = System.nanoTime();
                        TransformOutcome transformOutcome = transformRecords(ctx, entity, window);
                        List<PreparedRecord> preparedRecords = transformOutcome.preparedRecords();
                        recordsTransformed += transformOutcome.transformedRecords();
                        rowsProcessed += transformOutcome.transformedRecords();
                        recordsDiscarded += transformOutcome.discardedRecords().size();
                        recordsStaged += transformOutcome.stagingRecords().size();
                        ctx.addRecordsConsolidated(transformOutcome.consolidatedRecords());
                        long operationRowsInserted = 0;
                        long operationRowsStaged = transformOutcome.stagingRecords().size();
                        long operationRowsDiscarded = transformOutcome.discardedRecords().size();
                        long operationRowsTransformed = preparedRecords.size();

                        Instant maxTimestampRows = resolveCheckpointTimestamp(cursor, window, entity, preparedRecords,
                                transformOutcome.interruptedByGuardrail());
                        Instant checkpointToPersist = preparedRecords.isEmpty() ? cursor : maxTimestampRows;
                        ctx.setIngestorLogicDurationMs(ctx.getIngestorLogicDurationMs()
                                + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ingestorLogicStartNs));

                        if (!preparedRecords.isEmpty() || !transformOutcome.stagingRecords().isEmpty() || !transformOutcome.discardedRecords().isEmpty()) {
                            List<Object> payload = preparedRecords.stream()
                                    .map(PreparedRecord::transformedRecord)
                                    .collect(Collectors.toList());

                            long postgresInsertStartNs = System.nanoTime();
                            LogHelper.info(ctx, "PERSIST_ATTEMPT",
                                    "writing window to Postgres: rows={} from={} to={} checkpointTs={} operationId={}",
                                    payload.size(), window.getFromInclusive(), window.getToExclusive(),
                                    checkpointToPersist, ctx.getOperationId());
                            try {
                                WindowCyclePersistenceService.WindowCycleResult cycleResult =
                                        windowCyclePersistenceService.persistWindowCycle(
                                                ctx,
                                                entity,
                                                payload,
                                                transformOutcome.stagingRecords(),
                                                transformOutcome.discardedRecords(),
                                                checkpointToPersist
                                        );
                                recordsInserted += cycleResult.rowsInserted();
                                operationRowsInserted = cycleResult.rowsInserted();
                                if (cycleResult.rowsInserted() > 0) {
                                    executionLogService.updateLatestCheckpoint(ctx, checkpointToPersist);
                                }
                                LogHelper.info(ctx, RunPhase.CHECKPOINT,
                                        "operation cycle persisted: rowsInserted=" + cycleResult.rowsInserted()
                                                + ", rowsStaged=" + cycleResult.rowsStaged()
                                                + ", checkpointTs=" + checkpointToPersist
                                                + ", operationId=" + ctx.getOperationId());
                            } catch (Exception persistEx) {
                                LogHelper.error(ctx, RunPhase.ERROR,
                                        "persistWindowCycle failed: " + buildDetailedErrorMessage(persistEx));
                                throw persistEx;
                            } finally {
                                long persistMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - postgresInsertStartNs);
                                ctx.setPostgresInsertDurationMs(ctx.getPostgresInsertDurationMs() + persistMs);
                                Duration slowThreshold = ingestionConfig.getPersistence() != null
                                        ? ingestionConfig.getPersistence().getSlowWriteWarnThreshold() : null;
                                if (slowThreshold != null && !slowThreshold.isZero() && !slowThreshold.isNegative()
                                        && persistMs > slowThreshold.toMillis()) {
                                    LogHelper.warn(ctx, "SLOW_PERSIST",
                                            "Postgres window write slow: durationMs={} rows={} thresholdMs={} operationId={}",
                                            persistMs, payload.size(), slowThreshold.toMillis(), ctx.getOperationId());
                                }
                            }
                        } else {
                            LogHelper.info(ctx, RunPhase.CHECKPOINT,
                                    "operation cycle had no transformed/staged/discarded rows: checkpoint unchanged at=" + cursor
                                            + ", operationId=" + ctx.getOperationId());
                        }

                        if (transformOutcome.interruptedByGuardrail()) {
                            endReason = END_REASON_GUARDRAIL_MAX_DURATION;
                        }

                        // Guardrail stop with no transformed rows: keep cursor/checkpoint unchanged.
                        if (transformOutcome.interruptedByGuardrail() && preparedRecords.isEmpty()) {
                            runWindowToTs = cursor;
                            LogHelper.warn(ctx, RunPhase.SKIP,
                                    "Guardrail max duration reached before transforming rows, ending run without cursor advance");
                            break;
                        }

                        cursor = advanceCursor(cursor, maxTimestampRows, endLimit);
                        runWindowToTs = cursor;
                        LogHelper.info(ctx, RunPhase.WINDOW, "queriesExecuted=" + queriesExecuted + ", rowsProcessed=" + rowsProcessed + ", cursor=" + cursor + ", maxTimestampRows=" + maxTimestampRows);
                        LogHelper.info(ctx, "OPERATION_SUMMARY",
                                "from={} to={} read={} transformed={} staged={} discarded={} deferred={} inserted={} checkpoint={}",
                                window.getFromInclusive(), window.getToExclusive(), extractedRows,
                                operationRowsTransformed, operationRowsStaged, operationRowsDiscarded,
                                transformOutcome.deferredRecords(), operationRowsInserted, checkpointToPersist);

                        if (transformOutcome.interruptedByGuardrail()) {
                            LogHelper.warn(ctx, RunPhase.SKIP, "Guardrail max duration reached during transform, ending run after current operationId cycle");
                            break;
                        }
                        if (!preparedRecords.isEmpty() && isMaxDurationExceeded(ctx)) {
                            endReason = END_REASON_GUARDRAIL_MAX_DURATION;
                            LogHelper.warn(ctx, RunPhase.SKIP, "Guardrail max duration reached after persistence, ending run after current operationId cycle");
                            break;
                        }
                    } // end while

                    runWindowToTs = cursor;
                } // end else: cursor.isBefore(endLimit)
            }

            if (runWindowFromTs != null && runWindowToTs != null) {
                executionLogService.updateRunWindow(ctx, runWindowFromTs, runWindowToTs);
            }

            LogHelper.info(ctx, RunPhase.END,
                    "operationCount=" + operationCount
                            + ", queryCount=" + queriesExecuted
                            + ", recordsRead=" + recordsRead
                            + ", recordsTransformed=" + recordsTransformed
                            + ", recordsInserted=" + recordsInserted
                            + ", recordsDiscarded=" + recordsDiscarded
                            + ", recordsStaged=" + recordsStaged
                            + ", rowsProcessed=" + rowsProcessed
                            + ", adxQueryDurationMs=" + ctx.getAdxQueryDurationMs()
                            + ", ingestorLogicDurationMs=" + ctx.getIngestorLogicDurationMs()
                            + ", postgresInsertDurationMs=" + ctx.getPostgresInsertDurationMs()
                            + ", endReason=" + endReason);

            // Log execution completion
            executionLogService.logCompleted(
                    ctx,
                    recordsRead,
                    recordsTransformed,
                    recordsInserted,
                    recordsDiscarded,
                    recordsStaged,
                    queriesExecuted,
                    operationCount,
                    endReason
            );

        } catch (Throwable e) {
            String detailedError = buildDetailedErrorMessage(e);
            LogHelper.error(ctx, RunPhase.ERROR, "Unhandled exception: " + detailedError);
            if (runWindowFromTs != null && runWindowToTs != null) {
                executionLogService.updateRunWindow(ctx, runWindowFromTs, runWindowToTs);
            }
            // Log execution failure
            executionLogService.logFailed(
                    ctx,
                    e.getClass().getSimpleName(),
                    detailedError,
                    recordsRead,
                    recordsTransformed,
                    recordsInserted,
                    recordsDiscarded,
                    recordsStaged,
                    queriesExecuted,
                    operationCount
            );
            throw new RuntimeException(e);
        } finally {
            LogHelper.info(ctx, RunPhase.END, "run completed");
        }
    }

    private Instant determineCursor(RunContext ctx, EntityName entity) {
        Optional<Instant> checkpointOpt = checkpointStore.getCheckpoint(entity);

        if (checkpointOpt.isPresent()) {
            Instant checkpoint = checkpointOpt.orElseThrow(
                    () -> new IllegalStateException("Checkpoint unexpectedly absent for entity=" + entity.name()));
            LogHelper.info(ctx, RunPhase.CHECKPOINT, "cursor set from checkpoint: " + checkpoint);
            return checkpoint;
        }

        Optional<EntityName> parentEntityOpt = resolveParentEntityForStartCursor(entity);
        if (parentEntityOpt.isPresent()) {
            EntityName parentEntity = parentEntityOpt.orElseThrow(
                    () -> new IllegalStateException("Parent entity unexpectedly absent for " + entity.name()));
            Optional<Instant> parentCheckpointOpt = checkpointStore.getCheckpoint(parentEntity);
            if (parentCheckpointOpt.isPresent()) {
                Instant parentCheckpoint = parentCheckpointOpt.orElseThrow(
                        () -> new IllegalStateException("Parent checkpoint unexpectedly absent for " + parentEntity.name()));
                Optional<Instant> oldestAdxTs = oldestTimestampProvider.getOldestTimestamp(ctx, entity);
                if (oldestAdxTs.isPresent()) {
                    Instant adxOldest = oldestAdxTs.orElseThrow(
                            () -> new IllegalStateException("ADX oldest timestamp unexpectedly absent for " + entity.name()));
                    // For child entities: start from the OLDEST valid point within parent's window.
                    // Use min(adxOldest, parentCheckpoint) to ensure cursor ≤ endLimit.
                    Instant firstRunCursor = adxOldest.isBefore(parentCheckpoint) ? adxOldest : parentCheckpoint;
                    if (!firstRunCursor.equals(adxOldest)) {
                        // Parent hasn't caught up to child's ADX oldest yet
                        LogHelper.info(ctx, RunPhase.CHECKPOINT,
                                "No checkpoint for entity; parent checkpoint is earlier than child's ADX oldest:"
                                        + " entityOldestAdx=" + adxOldest + ", parentEntity=" + parentEntity.name()
                                        + ", parentCheckpoint=" + parentCheckpoint + ", cursor=" + firstRunCursor);
                    } else {
                        // Parent has advanced past child's first record, or they're equal
                        LogHelper.info(ctx, RunPhase.CHECKPOINT,
                                "No checkpoint for entity; first-run cursor set to child's ADX oldest:"
                                        + " entityOldestAdx=" + adxOldest + ", parentEntity=" + parentEntity.name()
                                        + ", parentCheckpoint=" + parentCheckpoint + ", cursor=" + firstRunCursor);
                    }
                    return firstRunCursor;
                }
                // No ADX oldest available: fall back to parent checkpoint.
                LogHelper.info(ctx, RunPhase.CHECKPOINT,
                        "No checkpoint for entity and no ADX oldest available; first-run cursor set from parent checkpoint:"
                                + " parentEntity=" + parentEntity.name() + ", parentCheckpoint=" + parentCheckpoint);
                return parentCheckpoint;
            }
        }

        Instant reference = Optional.ofNullable(ctx.getRunStart()).orElse(Instant.now());
        Instant firstRunStart = Optional.ofNullable(ingestionConfig.getFirstRunStart()).orElse(reference);
        Optional<Instant> oldestAdxTs = oldestTimestampProvider.getOldestTimestamp(ctx, entity);
        if (oldestAdxTs.isPresent()) {
            Instant oldestAdx = oldestAdxTs.orElseThrow(
                    () -> new IllegalStateException("ADX oldest timestamp unexpectedly absent for bootstrap"));
            Instant bootstrapCursor = oldestAdx.isAfter(firstRunStart) ? oldestAdx : firstRunStart;
            LogHelper.info(ctx, RunPhase.CHECKPOINT,
                    "No checkpoint found, cursor set from ADX oldest timestamp (capped by first-run start): oldestAdx="
                            + oldestAdx + ", firstRunStart=" + firstRunStart + ", cursor=" + bootstrapCursor);
            return bootstrapCursor;
        }

        LogHelper.warn(ctx, RunPhase.CHECKPOINT,
                "No checkpoint found and ADX oldest timestamp unavailable, using first-run start cursor=" + firstRunStart);
        return firstRunStart;
    }

    private Optional<EntityName> resolveParentEntityForStartCursor(EntityName entity) {
        return switch (entity) {
            case POSITION_TOKENS -> Optional.of(EntityName.POSITION);
            case POSITION_TRANSFERS, EXTRA_INFO, EVENTS_WF -> Optional.of(EntityName.POSITION_TOKENS);
            default -> Optional.empty();
        };
    }

    private Optional<FirstRunParentGap> resolveFirstRunParentGap(RunContext ctx, EntityName entity) {
        if (checkpointStore.getCheckpoint(entity).isPresent()) {
            return Optional.empty();
        }
        Optional<EntityName> parentEntityOpt = resolveParentEntityForStartCursor(entity);
        if (parentEntityOpt.isEmpty()) {
            return Optional.empty();
        }
        EntityName parentEntity = parentEntityOpt.orElseThrow(
                () -> new IllegalStateException("Parent entity unexpectedly absent for " + entity.name()));
        Optional<Instant> parentCheckpointOpt = checkpointStore.getCheckpoint(parentEntity);
        Optional<Instant> childOldestAdxOpt = oldestTimestampProvider.getOldestTimestamp(ctx, entity);
        if (parentCheckpointOpt.isEmpty() || childOldestAdxOpt.isEmpty()) {
            return Optional.empty();
        }
        Instant parentCheckpoint = parentCheckpointOpt.orElseThrow(
                () -> new IllegalStateException("Parent checkpoint unexpectedly absent for " + parentEntity.name()));
        Instant childOldestAdx = childOldestAdxOpt.orElseThrow(
                () -> new IllegalStateException("Child oldest ADX timestamp unexpectedly absent for " + entity.name()));
        if (childOldestAdx.isAfter(parentCheckpoint)) {
            return Optional.of(new FirstRunParentGap(parentEntity, parentCheckpoint, childOldestAdx));
        }
        return Optional.empty();
    }

    private TransformOutcome transformRecords(RunContext ctx, EntityName entity, AdxWindowResult window) {
        List<PreparedRecord> preparedRecords = new ArrayList<>();
        List<WindowCyclePersistenceService.StagingRecord> stagingRecords = new ArrayList<>();
        List<WindowCyclePersistenceService.DiscardedRecord> discardedRecords = new ArrayList<>();
        Map<Integer, Integer> pendingPositionRecordIndexes = new HashMap<>();
        boolean interruptedByGuardrail = false;
        int transformedRecords = 0;
        int consolidatedRecords = 0;
        int deferredRecords = 0;
        BatchLocalCache batchCache = ctx.getBatchLocalCache();

        runWindowFkPrefetch(ctx, entity, window, batchCache);

        // Counter for synthetic IDs during transformation (for cache population of INSERT records)
        int syntheticIdCounter = -1;

        for (Map.Entry<String, Object> entry : getDeterministicRows(window.getRows())) {
            if (isMaxDurationExceeded(ctx)) {
                interruptedByGuardrail = true;
                break;
            }
            Object value = entry.getValue();
            if (!(value instanceof Map<?, ?> rawRow)) {
                LogHelper.error(ctx, RunPhase.ERROR, "Unexpected row payload type for key=" + entry.getKey());
                Map<String, Object> fallbackPayload = new java.util.HashMap<>();
                fallbackPayload.put("raw", value);
                stagingRecords.add(new WindowCyclePersistenceService.StagingRecord(
                        entry.getKey(),
                        fallbackPayload,
                        new IllegalArgumentException("Unexpected row payload type")
                ));
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rawRow;

            // EVENTS_WF: le query RESP/receipt leggono fino a `to + grace(5min)`. Le righe il cui
            // timestamp-àncora (REQ, o RESP per le receipt standalone) è >= fine finestra appartengono
            // alla zona di grace e verranno lette dalla finestra successiva. Differirle qui evita sia la
            // perdita dati (overshoot del cursore oltre `to`) sia i duplicati (EVENTS_WF non ha ON CONFLICT).
            if (entity == EntityName.EVENTS_WF) {
                Optional<Instant> anchor = extractInsertedTimestamp(row);
                if (anchor.isPresent() && !anchor.get().isBefore(window.getToExclusive())) {
                    deferredRecords++;
                    if (log.isDebugEnabled()) {
                        LogHelper.debug(ctx, "DEFER",
                                "grace-zone row deferred to next window: uniqueId=" + entry.getKey()
                                        + " token=" + extractTokenForLog(row)
                                        + " anchorTs=" + anchor.get()
                                        + " windowTo=" + window.getToExclusive());
                    }
                    continue;
                }
            }

            try {
                Object transformed = entityTransformer.transform(row, getTargetClass(entity), ctx, entity);

                if (isExtraInfoInfoNameToDiscard(entity, transformed)) {
                    ExtraInfo extraInfo = (ExtraInfo) transformed;
                    LogHelper.info(ctx, RunPhase.NOOP,
                            "EXTRA_INFO discarded because not in whitelist: infoName=" + extraInfo.getInfoName());
                    discardedRecords.add(new WindowCyclePersistenceService.DiscardedRecord(
                            entry.getKey(),
                            row,
                            DISCARD_REASON_EXTRA_INFO_NOT_WHITELIST
                    ));
                    continue;
                }

                // Eagerly populate cache during transform for POSITION/POSITION_TOKENS
                // This ensures subsequent records in the same batch find prior records in cache
                if (transformed instanceof it.pagopa.cruscotto.ingestion.entity.Position position) {
                    Instant sourceTimestamp = extractInsertedTimestamp(row).orElse(window.getToExclusive());
                    if (position.getId() != null && position.getId() < 0) {
                        Integer pendingRecordIndex = pendingPositionRecordIndexes.get(position.getId());
                        if (pendingRecordIndex == null) {
                            throw new IllegalStateException("Missing pending POSITION for synthetic id=" + position.getId());
                        }
                        PreparedRecord pendingRecord = preparedRecords.get(pendingRecordIndex);
                        Position pendingPosition = (Position) pendingRecord.transformedRecord();
                        mergePendingPosition(pendingPosition, position);
                        if (sourceTimestamp.isAfter(pendingRecord.insertedTimestamp())) {
                            preparedRecords.set(pendingRecordIndex, new PreparedRecord(
                                    pendingPosition, row, sourceTimestamp, entry.getKey()));
                        }
                        transformedRecords++;
                        consolidatedRecords++;
                        continue;
                    }

                    int cacheId = position.getId() != null ? position.getId() : syntheticIdCounter--;
                    if (position.getNav() != null && position.getPaEmittente() != null &&
                        position.getInsertedTimestamp() != null) {
                        batchCache.cachePosition(cacheId, position.getNav(),
                            position.getPaEmittente(), position.getInsertedTimestamp());
                    }
                    if (position.getId() == null) {
                        pendingPositionRecordIndexes.put(cacheId, preparedRecords.size());
                    }
                } else if (transformed instanceof it.pagopa.cruscotto.ingestion.entity.PositionTokens token) {
                    if (token.getId() != null && token.getToken() != null) {
                        String tokenBase64 = java.util.Base64.getEncoder().encodeToString(token.getToken());
                        batchCache.cacheToken(tokenBase64, token.getId());
                    }
                }

                preparedRecords.add(new PreparedRecord(transformed, row, extractInsertedTimestamp(row).orElse(window.getToExclusive()), entry.getKey()));
                transformedRecords++;
                if (entity == EntityName.EVENTS_WF && log.isDebugEnabled() && transformed instanceof EventsWf tracedEvent) {
                    LogHelper.debug(ctx, "EVENT_TRACE",
                            "prepared uniqueId=" + entry.getKey()
                                    + " token=" + extractTokenForLog(row)
                                    + " fkPosition=" + tracedEvent.getFkPosition()
                                    + " fkTokens=" + tracedEvent.getFkTokens()
                                    + " anchorTs=" + extractInsertedTimestamp(row).orElse(null));
                }
            } catch (EntityTransformer.TransformationException e) {
                String detailedError = buildDetailedErrorMessage(e);
                LogHelper.error(ctx, RunPhase.ERROR, "Transformation failed for uniqueId=" + entry.getKey()
                        + " token=" + extractTokenForLog(row) + ": " + detailedError);

                // SQL/DB errors must fail fast: staging insert would be rejected in aborted transaction
                if (isSqlFailure(e)) {
                    throw new RuntimeException("TRANSFORM_SQL_ERROR rowKey=" + entry.getKey() + " " + detailedError, e);
                }
                stagingRecords.add(new WindowCyclePersistenceService.StagingRecord(entry.getKey(), row, e));
            }
        }
        int discardedCount = discardedRecords.size();
        // Clear window-scoped prefetch to keep memory bounded; run-level caches are not affected.
        batchCache.clearWindowPrefetch();
        return new TransformOutcome(preparedRecords, stagingRecords, discardedRecords, discardedCount, transformedRecords, consolidatedRecords, deferredRecords, interruptedByGuardrail);
    }

    private void runWindowFkPrefetch(RunContext ctx, EntityName entity, AdxWindowResult window, BatchLocalCache batchCache) {
        if (window == null || window.getRows() == null || window.getRows().isEmpty() || batchCache == null) {
            return;
        }

        // Window-scoped FK prefetch: batch-resolve FKs before the per-record loop to
        // reduce N individual DB round-trips to M (one per unique business key).
        // Only positive results are cached; misses always fall through to individual resolvers.
        if (entity == EntityName.POSITION_TOKENS) {
            positionFkBatchPrefetcher.prefetchForPositionTokens(window.getRows(), batchCache, ctx);
            return;
        }
        if (entity == EntityName.POSITION_TRANSFERS) {
            tokenFkBatchPrefetcher.prefetchForPositionTransfers(window.getRows(), batchCache, ctx);
            return;
        }
        if (entity != EntityName.EVENTS_WF) {
            return;
        }

        IngestionConfig.EventsWfConfig.PrefetchConfig prefetch = ingestionConfig.getEventsWf().getPrefetch();
        if (prefetch == null || !prefetch.isEnabled()) {
            return;
        }

        int tokenThreshold = Math.max(1, prefetch.getMinDistinctTokenKeys());
        boolean tokenPrefetchExecuted = countDistinctEventTokenKeys(window.getRows()) >= tokenThreshold;
        if (tokenPrefetchExecuted) {
            tokenFkBatchPrefetcher.prefetchForPositionTransfers(window.getRows(), batchCache, ctx);
        }

        Map<String, Object> rowsForPositionPrefetch = selectEventsRowsForPositionPrefetch(
                window.getRows(),
                batchCache,
                tokenPrefetchExecuted
        );
        int positionThreshold = Math.max(1, prefetch.getMinDistinctPositionKeys());
        if (countDistinctPositionKeys(rowsForPositionPrefetch) >= positionThreshold) {
            positionFkBatchPrefetcher.prefetchForPositionTokens(rowsForPositionPrefetch, batchCache, ctx);
        }
    }

    private int countDistinctEventTokenKeys(Map<String, Object> rows) {
        Set<String> distinctTokens = new LinkedHashSet<>();
        for (Object value : rows.values()) {
            if (!(value instanceof Map<?, ?> rawRow)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rawRow;
            String tokenBase64 = tokenBase64(row);
            if (tokenBase64 != null) {
                distinctTokens.add(tokenBase64);
            }
        }
        return distinctTokens.size();
    }

    private Map<String, Object> selectEventsRowsForPositionPrefetch(Map<String, Object> rows,
                                                                     BatchLocalCache batchCache,
                                                                     boolean tokenPrefetchExecuted) {
        Map<String, Object> selected = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : getDeterministicRows(rows)) {
            if (!(entry.getValue() instanceof Map<?, ?> rawRow)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rawRow;
            String tokenBase64 = tokenBase64(row);
            if (tokenBase64 == null) {
                selected.put(entry.getKey(), row);
                continue;
            }
            if (!tokenPrefetchExecuted) {
                continue;
            }
            if (!batchCache.hasTokenWindowPrefetch(tokenBase64)) {
                selected.put(entry.getKey(), row);
            }
        }
        return selected;
    }

    private int countDistinctPositionKeys(Map<String, Object> rows) {
        Set<String> distinctKeys = new LinkedHashSet<>();
        for (Object value : rows.values()) {
            if (!(value instanceof Map<?, ?> rawRow)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rawRow;
            String nav = stringValue(row, "NAV", "nav");
            String paEmittente = stringValue(row, "PA_EMITTENTE", "pa_emittente", "paEmittente");
            LocalDateTime insertedTimestamp = insertedTimestampValue(row);
            if (nav != null && paEmittente != null && insertedTimestamp != null) {
                distinctKeys.add(nav + "|" + paEmittente + "|" + insertedTimestamp);
            }
        }
        return distinctKeys.size();
    }

    private String tokenBase64(Map<String, Object> row) {
        Object token = row.get("TOKEN");
        if (token == null) {
            token = row.get("token");
        }
        byte[] tokenBytes;
        if (token instanceof byte[] bytes) {
            tokenBytes = bytes;
        } else if (token instanceof String value) {
            if (value.isBlank()) {
                return null;
            }
            tokenBytes = value.getBytes(StandardCharsets.UTF_8);
        } else if (token != null) {
            tokenBytes = String.valueOf(token).getBytes(StandardCharsets.UTF_8);
        } else {
            return null;
        }
        return Base64.getEncoder().encodeToString(tokenBytes);
    }

    private String stringValue(Map<String, Object> row, String... keys) {
        for (String key : keys) {
            Object value = row.get(key);
            if (value instanceof String string && !string.isBlank()) {
                return string;
            }
            if (value != null) {
                return String.valueOf(value);
            }
        }
        return null;
    }

    private LocalDateTime insertedTimestampValue(Map<String, Object> row) {
        return toInstant(firstNonNull(
                row,
                "INSERTED_TIMESTAMP_RESP", "inserted_timestamp_resp", "insertedTimestampResp",
                "INSERTED_TIMESTAMP_REQ", "inserted_timestamp_req", "insertedTimestampReq",
                "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp"
        ))
                .map(instant -> LocalDateTime.ofInstant(instant, ZoneOffset.UTC))
                .orElse(null);
    }

    private Object firstNonNull(Map<String, Object> row, String... keys) {
        for (String key : keys) {
            Object value = row.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private void mergePendingPosition(Position pendingPosition, Position duplicatePosition) {
        LocalDateTime duplicateLastEvent = duplicatePosition.getLastEvent();
        if (duplicateLastEvent != null
                && (pendingPosition.getLastEvent() == null || duplicateLastEvent.isAfter(pendingPosition.getLastEvent()))) {
            pendingPosition.setLastEvent(duplicateLastEvent);
        }
        if ((pendingPosition.getDateEvents() == null || "[]".equals(pendingPosition.getDateEvents()))
                && duplicatePosition.getDateEvents() != null) {
            pendingPosition.setDateEvents(duplicatePosition.getDateEvents());
        }
    }

    private boolean isExtraInfoInfoNameToDiscard(EntityName entity, Object transformed) {
        if (entity != EntityName.EXTRA_INFO || !(transformed instanceof ExtraInfo extraInfo)) {
            return false;
        }
        return !extraInfoWhitelistService.isAllowed(extraInfo.getInfoName());
    }

    private boolean isSqlFailure(Throwable throwable) {
        Throwable cursor = throwable;
        while (cursor != null) {
            if (cursor instanceof SQLException || cursor instanceof DataAccessException) {
                return true;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private String buildDetailedErrorMessage(Throwable throwable) {
        StringJoiner joiner = new StringJoiner(" | causedBy=");
        Throwable cursor = throwable;
        int depth = 0;
        while (cursor != null && depth < 5) {
            String message = cursor.getMessage();
            if (message != null && !message.isBlank()) {
                joiner.add(cursor.getClass().getSimpleName() + ": " + message);
            }
            cursor = cursor.getCause();
            depth++;
        }
        String rendered = joiner.toString();
        return rendered.isBlank() ? throwable.getClass().getSimpleName() : rendered;
    }

    private List<Map.Entry<String, Object>> getDeterministicRows(Map<String, Object> rows) {
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }

        return rows.entrySet().stream()
                .sorted(Comparator
                        .comparing((Map.Entry<String, Object> entry) -> extractInsertedTimestamp(entry.getValue()).orElse(Instant.EPOCH))
                        .thenComparing(Map.Entry::getKey))
                .collect(Collectors.toList());
    }

    private Instant resolveCheckpointTimestamp(Instant currentCursor,
                                               AdxWindowResult window,
                                               EntityName entity,
                                               List<PreparedRecord> preparedRecords,
                                               boolean interruptedByGuardrail) {
        if (interruptedByGuardrail) {
            return preparedRecords.stream()
                    .map(PreparedRecord::insertedTimestamp)
                    .filter(Objects::nonNull)
                    .max(Comparator.naturalOrder())
                    .orElse(currentCursor);
        }

        // EVENTS_WF: le query RESP/receipt leggono fino a `to + grace(5min)`, quindi tra le righe
        // preparate possono esserci RESP con timestamp oltre `to`. Avanzare il cursore al max di quei
        // timestamp salterebbe l'intervallo REQ [to, to+grace] alla finestra successiva (perdita dati).
        // Le righe della zona di grace sono differite in transformRecords: qui fissiamo il checkpoint a
        // fine finestra così la copertura resta contigua e senza buchi.
        if (entity == EntityName.EVENTS_WF) {
            return window.getToExclusive();
        }

        Instant maxTimestampFromSourceRows = getDeterministicRows(window.getRows()).stream()
                .map(Map.Entry::getValue)
                .map(this::extractInsertedTimestamp)
                .flatMap(Optional::stream)
                .max(Comparator.naturalOrder())
                .orElse(window.getToExclusive());

        return preparedRecords.stream()
                .map(PreparedRecord::insertedTimestamp)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder())
                .orElse(maxTimestampFromSourceRows);
    }

    private String extractTokenForLog(Map<String, Object> row) {
        if (row == null) {
            return "n/a";
        }
        for (String key : List.of("TOKEN", "token", "Token")) {
            Object value = row.get(key);
            if (value != null) {
                String rendered = String.valueOf(value).trim();
                if (!rendered.isEmpty()) {
                    return rendered;
                }
            }
        }
        return "n/a";
    }

    private Optional<Instant> extractInsertedTimestamp(Object source) {
        if (!(source instanceof Map<?, ?> rawMap)) {
            return Optional.empty();
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) rawMap;
        return extractInsertedTimestamp(row);
    }

    private Optional<Instant> extractInsertedTimestamp(Map<String, Object> row) {
        for (String key : List.of(
                "inserted_timestamp",
                "insertedTimestamp",
                "INSERTED_TIMESTAMP",
                "inserted_timestamp_req",
                "insertedTimestampReq",
                "INSERTED_TIMESTAMP_REQ",
                "inserted_timestamp_resp",
                "insertedTimestampResp",
                "INSERTED_TIMESTAMP_RESP"
        )) {
            Optional<Instant> instant = toInstant(row.get(key));
            if (instant.isPresent()) {
                return instant;
            }
        }

        return Optional.empty();
    }

    private Optional<Instant> toInstant(Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof Instant instant) {
            return Optional.of(instant);
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return Optional.of(offsetDateTime.toInstant());
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return Optional.of(zonedDateTime.toInstant());
        }
        if (value instanceof LocalDateTime localDateTime) {
            return Optional.of(localDateTime.toInstant(ZoneOffset.UTC));
        }
        if (value instanceof Date date) {
            return Optional.of(date.toInstant());
        }
        if (value instanceof Number number) {
            return Optional.of(Instant.ofEpochMilli(number.longValue()));
        }
        if (value instanceof CharSequence charSequence) {
            String timestamp = charSequence.toString().trim();
            if (timestamp.isEmpty()) {
                return Optional.empty();
            }
            try {
                return Optional.of(Instant.parse(timestamp));
            } catch (DateTimeParseException ignored) {
                try {
                    return Optional.of(OffsetDateTime.parse(timestamp).toInstant());
                } catch (DateTimeParseException ignoredAgain) {
                    try {
                        return Optional.of(LocalDateTime.parse(timestamp).toInstant(ZoneOffset.UTC));
                    } catch (DateTimeParseException ignoredThird) {
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.empty();
    }

    private Instant min(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    private Instant advanceCursor(Instant currentCursor, Instant maxTimestampRows, Instant endLimit) {
        Instant candidate = min(maxTimestampRows, endLimit);
        if (!candidate.isAfter(currentCursor) && currentCursor.isBefore(endLimit)) {
            Instant bumped = currentCursor.plusMillis(1);
            return min(bumped, endLimit);
        }
        return candidate;
    }

    private String resolveWindowProfile(EntityName entity, Duration configuredWindow) {
        if (entity != EntityName.EVENTS_WF) {
            return WINDOW_PROFILE_STANDARD;
        }
        Duration realtimeWindow = ingestionConfig.getInitialWindow(EntityName.EVENTS_WF);
        if (!Objects.equals(configuredWindow, realtimeWindow)) {
            return WINDOW_PROFILE_CATCH_UP;
        }
        return WINDOW_PROFILE_REALTIME;
    }

    private boolean isMaxDurationExceeded(RunContext ctx) {
        IngestionConfig.GuardrailsConfig guardrails = ingestionConfig.getGuardrails();
        if (!guardrails.isEnableMaxDuration() || ctx.getRunStart() == null) {
            return false;
        }
        Duration elapsed = Duration.between(ctx.getRunStart(), Instant.now());
        Duration maxDuration = ingestionConfig.resolveMaxDurationForRun(ctx.getEntityName(), ctx.isCatchupMode());
        return elapsed.compareTo(maxDuration) > 0;
    }

    private String resolveGuardrailEndReason(RunContext ctx, long queriesExecuted, long rowsProcessed) {
        IngestionConfig.GuardrailsConfig guardrails = ingestionConfig.getGuardrails();
        if (guardrails.isEnableMaxDuration() && ctx.getRunStart() != null) {
            Duration elapsed = Duration.between(ctx.getRunStart(), Instant.now());
            Duration maxDuration = ingestionConfig.resolveMaxDurationForRun(ctx.getEntityName(), ctx.isCatchupMode());
            if (elapsed.compareTo(maxDuration) > 0) {
                return END_REASON_GUARDRAIL_MAX_DURATION;
            }
        }
        if (guardrails.isEnableMaxQueries() && queriesExecuted >= guardrails.getMaxQueries()) {
            return END_REASON_GUARDRAIL_MAX_QUERIES;
        }
        if (guardrails.isEnableMaxRows() && rowsProcessed >= guardrails.getMaxRows()) {
            return END_REASON_GUARDRAIL_MAX_ROWS;
        }
        return END_REASON_GUARDRAIL_LIMIT;
    }

    private Class<?> getTargetClass(EntityName entityName) {
        return switch (entityName) {
            case POSITION -> Position.class;
            case POSITION_TOKENS -> PositionTokens.class;
            case POSITION_TRANSFERS -> PositionTransfers.class;
            case EXTRA_INFO -> ExtraInfo.class;
            case EVENTS_WF -> EventsWf.class;
            default -> throw new IllegalArgumentException("No target class configured for entity: " + entityName);
        };
    }

    private record PreparedRecord(Object transformedRecord, Map<String, Object> sourceRow, Instant insertedTimestamp, String sourceKey) {
    }

    private record TransformOutcome(
            List<PreparedRecord> preparedRecords,
            List<WindowCyclePersistenceService.StagingRecord> stagingRecords,
            List<WindowCyclePersistenceService.DiscardedRecord> discardedRecords,
            int discardedCount,
            int transformedRecords,
            int consolidatedRecords,
            int deferredRecords,
            boolean interruptedByGuardrail) {
    }

    private record FirstRunParentGap(EntityName parentEntity, Instant parentCheckpoint, Instant childOldestAdx) {
    }
}
