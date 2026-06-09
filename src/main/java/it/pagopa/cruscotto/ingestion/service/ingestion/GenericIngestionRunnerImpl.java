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
import it.pagopa.cruscotto.ingestion.service.RunGuardrails;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryService;
import it.pagopa.cruscotto.ingestion.service.adx.AdxWindowResult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
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

    private final CheckpointStoreService checkpointStore;
    private final EndLimitResolverService endLimitResolver;
    private final RunGuardrails runGuardrails;
    private final AdxQueryService adxQueryService;
    private final OldestTimestampProvider oldestTimestampProvider;
    private final ExecutionLogService executionLogService;
    private final WindowCyclePersistenceService windowCyclePersistenceService;
    private final IngestionConfig ingestionConfig;
    private final EntityTransformer entityTransformer;

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
                Instant endLimit = endLimitOpt.get();

                Instant cursor = determineCursor(ctx, entity);

                // Guard: if the cursor is already at or past the endLimit there is nothing to do
                // this run (e.g. parent entity hasn't caught up to the child's ADX oldest yet).
                if (!cursor.isBefore(endLimit)) {
                    Optional<FirstRunParentGap> firstRunParentGap = resolveFirstRunParentGap(ctx, entity);
                    if (firstRunParentGap.isPresent()) {
                        FirstRunParentGap gap = firstRunParentGap.get();
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

                    // Record initial_ts BEFORE processing starts.
                    // Idempotent: the SQL COALESCE ensures it is never overwritten on subsequent runs.
                    checkpointStore.initializeInitialTs(entity, cursor, ctx.getRunId());

                    while (cursor.isBefore(endLimit)) {
                        if (!runGuardrails.ok(ctx, queriesExecuted, rowsProcessed)) {
                            endReason = resolveGuardrailEndReason(ctx, queriesExecuted, rowsProcessed);
                            LogHelper.warn(ctx, RunPhase.SKIP, "Guardrail stop detected, ending run with reason=" + endReason);
                            break;
                        }

                        ctx.setOperationId(UUID.randomUUID().toString());
                        operationCount++;
                        Duration configuredWindow = ingestionConfig.getInitialWindow(entity);
                        Optional<AdxWindowResult> windowOpt = adxQueryService.fetchWindow(
                                ctx,
                                cursor,
                                configuredWindow,
                                endLimit
                        );

                        if (windowOpt.isEmpty()) {
                            LogHelper.warn(ctx, RunPhase.WINDOW, "No result returned");
                            break;
                        }

                        AdxWindowResult window = windowOpt.get();
                        queriesExecuted += window.getAttempts();
                        int extractedRows = window.getRows() != null ? window.getRows().size() : 0;
                        recordsRead += extractedRows;
                        LogHelper.info(ctx, RunPhase.WINDOW,
                                "window extractedRows=" + extractedRows
                                        + ", from=" + window.getFromInclusive()
                                        + ", to=" + window.getToExclusive()
                                        + ", attempts=" + window.getAttempts());

                        if (window.getRows() == null || window.getRows().isEmpty()) {
                            cursor = min(cursor.plus(window.getWindowUsed()), endLimit);
                            runWindowToTs = cursor;
                            LogHelper.info(ctx, RunPhase.WINDOW, "empty rows, queriesExecuted=" + queriesExecuted + ", rowsProcessed=" + rowsProcessed + ", cursor=" + cursor);
                            continue;
                        }

                        TransformOutcome transformOutcome = transformRecords(ctx, entity, window);
                        List<PreparedRecord> preparedRecords = transformOutcome.preparedRecords();
                        recordsTransformed += preparedRecords.size();
                        rowsProcessed += preparedRecords.size();
                        recordsStaged += transformOutcome.stagingRecords().size();
                        long operationRowsInserted = 0;
                        long operationRowsStaged = transformOutcome.stagingRecords().size();
                        long operationRowsTransformed = preparedRecords.size();

                        Instant maxTimestampRows = resolveCheckpointTimestamp(cursor, window, preparedRecords,
                                transformOutcome.interruptedByGuardrail());
                        Instant checkpointToPersist = preparedRecords.isEmpty() ? cursor : maxTimestampRows;

                        if (!preparedRecords.isEmpty() || !transformOutcome.stagingRecords().isEmpty()) {
                            List<Object> payload = preparedRecords.stream()
                                    .map(PreparedRecord::transformedRecord)
                                    .collect(Collectors.toList());

                            try {
                                WindowCyclePersistenceService.WindowCycleResult cycleResult =
                                        windowCyclePersistenceService.persistWindowCycle(
                                                ctx,
                                                entity,
                                                payload,
                                                transformOutcome.stagingRecords(),
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
                            }
                        } else {
                            LogHelper.info(ctx, RunPhase.CHECKPOINT,
                                    "operation cycle had no transformed/staged rows: checkpoint unchanged at=" + cursor
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
                                "from={} to={} read={} transformed={} staged={} inserted={} checkpoint={}",
                                window.getFromInclusive(), window.getToExclusive(), extractedRows,
                                operationRowsTransformed, operationRowsStaged, operationRowsInserted, checkpointToPersist);

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
            Instant checkpoint = checkpointOpt.get();
            LogHelper.info(ctx, RunPhase.CHECKPOINT, "cursor set from checkpoint: " + checkpoint);
            return checkpoint;
        }

        Optional<EntityName> parentEntityOpt = resolveParentEntityForStartCursor(entity);
        if (parentEntityOpt.isPresent()) {
            EntityName parentEntity = parentEntityOpt.get();
            Optional<Instant> parentCheckpointOpt = checkpointStore.getCheckpoint(parentEntity);
            if (parentCheckpointOpt.isPresent()) {
                Instant parentCheckpoint = parentCheckpointOpt.get();
                Optional<Instant> oldestAdxTs = oldestTimestampProvider.getOldestTimestamp(ctx, entity);
                if (oldestAdxTs.isPresent()) {
                    Instant adxOldest = oldestAdxTs.get();
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
        Instant lookbackFloor = reference
                .atZone(ZoneOffset.UTC)
                .minus(ingestionConfig.getFirstRunLookback())
                .toInstant();
        Optional<Instant> oldestAdxTs = oldestTimestampProvider.getOldestTimestamp(ctx, entity);
        if (oldestAdxTs.isPresent()) {
            Instant bootstrapCursor = oldestAdxTs.get().isAfter(lookbackFloor) ? oldestAdxTs.get() : lookbackFloor;
            LogHelper.info(ctx, RunPhase.CHECKPOINT,
                    "No checkpoint found, cursor set from ADX oldest timestamp (capped by lookback floor): oldestAdx="
                            + oldestAdxTs.get() + ", lookbackFloor=" + lookbackFloor + ", cursor=" + bootstrapCursor);
            return bootstrapCursor;
        }

        LogHelper.warn(ctx, RunPhase.CHECKPOINT,
                "No checkpoint found and ADX oldest timestamp unavailable, using first-run lookback cursor=" + lookbackFloor);
        return lookbackFloor;
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
        EntityName parentEntity = parentEntityOpt.get();
        Optional<Instant> parentCheckpointOpt = checkpointStore.getCheckpoint(parentEntity);
        Optional<Instant> childOldestAdxOpt = oldestTimestampProvider.getOldestTimestamp(ctx, entity);
        if (parentCheckpointOpt.isEmpty() || childOldestAdxOpt.isEmpty()) {
            return Optional.empty();
        }
        Instant parentCheckpoint = parentCheckpointOpt.get();
        Instant childOldestAdx = childOldestAdxOpt.get();
        if (childOldestAdx.isAfter(parentCheckpoint)) {
            return Optional.of(new FirstRunParentGap(parentEntity, parentCheckpoint, childOldestAdx));
        }
        return Optional.empty();
    }

    private TransformOutcome transformRecords(RunContext ctx, EntityName entity, AdxWindowResult window) {
        List<PreparedRecord> preparedRecords = new ArrayList<>();
        List<WindowCyclePersistenceService.StagingRecord> stagingRecords = new ArrayList<>();
        boolean interruptedByGuardrail = false;
        BatchLocalCache batchCache = ctx.getBatchLocalCache();

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

            try {
                Object transformed = entityTransformer.transform(row, getTargetClass(entity), ctx, entity);

                // Eagerly populate cache during transform for POSITION/POSITION_TOKENS
                // This ensures subsequent records in the same batch find prior records in cache
                if (transformed instanceof it.pagopa.cruscotto.ingestion.entity.Position position) {
                    int cacheId = position.getId() != null ? position.getId() : syntheticIdCounter--;
                    if (position.getNav() != null && position.getPaEmittente() != null &&
                        position.getInsertedTimestamp() != null) {
                        batchCache.cachePosition(cacheId, position.getNav(),
                            position.getPaEmittente(), position.getInsertedTimestamp());
                    }
                } else if (transformed instanceof it.pagopa.cruscotto.ingestion.entity.PositionTokens token) {
                    if (token.getId() != null && token.getToken() != null) {
                        String tokenBase64 = java.util.Base64.getEncoder().encodeToString(token.getToken());
                        batchCache.cacheToken(tokenBase64, token.getId());
                    }
                }

                preparedRecords.add(new PreparedRecord(transformed, row, extractInsertedTimestamp(row).orElse(window.getToExclusive()), entry.getKey()));
            } catch (EntityTransformer.TransformationException e) {
                String detailedError = buildDetailedErrorMessage(e);
                LogHelper.error(ctx, RunPhase.ERROR, "Transformation failed for row key=" + entry.getKey() + ": " + detailedError);

                // SQL/DB errors must fail fast: staging insert would be rejected in aborted transaction
                if (isSqlFailure(e)) {
                    throw new RuntimeException("TRANSFORM_SQL_ERROR rowKey=" + entry.getKey() + " " + detailedError, e);
                }
                stagingRecords.add(new WindowCyclePersistenceService.StagingRecord(entry.getKey(), row, e));
            }
        }
        return new TransformOutcome(preparedRecords, stagingRecords, interruptedByGuardrail);
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
                                               List<PreparedRecord> preparedRecords,
                                               boolean interruptedByGuardrail) {
        if (interruptedByGuardrail) {
            return preparedRecords.stream()
                    .map(PreparedRecord::insertedTimestamp)
                    .filter(Objects::nonNull)
                    .max(Comparator.naturalOrder())
                    .orElse(currentCursor);
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

    private boolean isMaxDurationExceeded(RunContext ctx) {
        IngestionConfig.GuardrailsConfig guardrails = ingestionConfig.getGuardrails();
        if (!guardrails.isEnableMaxDuration() || ctx.getRunStart() == null) {
            return false;
        }
        Duration elapsed = Duration.between(ctx.getRunStart(), Instant.now());
        return elapsed.compareTo(guardrails.getMaxDuration()) > 0;
    }

    private String resolveGuardrailEndReason(RunContext ctx, long queriesExecuted, long rowsProcessed) {
        IngestionConfig.GuardrailsConfig guardrails = ingestionConfig.getGuardrails();
        if (guardrails.isEnableMaxDuration() && ctx.getRunStart() != null) {
            Duration elapsed = Duration.between(ctx.getRunStart(), Instant.now());
            if (elapsed.compareTo(guardrails.getMaxDuration()) > 0) {
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
            boolean interruptedByGuardrail) {
    }

    private record FirstRunParentGap(EntityName parentEntity, Instant parentCheckpoint, Instant childOldestAdx) {
    }
}
