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
import it.pagopa.cruscotto.ingestion.service.RunGuardrails;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryService;
import it.pagopa.cruscotto.ingestion.service.adx.AdxWindowResult;

import it.pagopa.cruscotto.ingestion.service.StagingErrorService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

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
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class GenericIngestionRunnerImpl implements GenericIngestionRunner {

    private final CheckpointStoreService checkpointStore;
    private final EndLimitResolverService endLimitResolver;
    private final RunGuardrails runGuardrails;
    private final AdxQueryService adxQueryService;
    private final StagingErrorService stagingErrorService;
    private final IngestionConfig ingestionConfig;
    private final OldestTimestampProvider oldestTimestampProvider;
    private final EntityTransformer entityTransformer;
    private final BulkWriter bulkWriter;

    @Override
    public void runEntity(RunContext ctx) {
        EntityName entity = EntityName.valueOf(ctx.getEntityName());

        LogHelper.info(ctx, RunPhase.START, "");

        long queriesExecuted = 0;
        long rowsProcessed = 0;

        try {
            // 1. Resolve endLimit
            Optional<Instant> endLimitOpt = endLimitResolver.resolveEndLimit(ctx);
            if (endLimitOpt.isEmpty()) {
                LogHelper.info(ctx, RunPhase.NOOP, "endLimit not resolved");
                return;
            }
            Instant endLimit = endLimitOpt.get();

            Instant cursor = determineCursor(ctx, entity, endLimit);

            while (cursor.isBefore(endLimit) && runGuardrails.ok(ctx, queriesExecuted, rowsProcessed)) {
                Optional<AdxWindowResult> windowOpt = adxQueryService.fetchWindow(
                        ctx,
                        cursor,
                        ingestionConfig.getInitialWindow(),
                        endLimit
                );

                if (windowOpt.isEmpty()) {
                    LogHelper.warn(ctx, RunPhase.WINDOW, "No result returned");
                    break;
                }

                AdxWindowResult window = windowOpt.get();
                queriesExecuted++;
                int extractedRows = window.getRows() != null ? window.getRows().size() : 0;
                LogHelper.info(ctx, RunPhase.WINDOW,
                        "window extractedRows=" + extractedRows
                                + ", from=" + window.getFromInclusive()
                                + ", to=" + window.getToExclusive()
                                + ", attempts=" + window.getAttempts());

                if (window.getRows() == null || window.getRows().isEmpty()) {
                    cursor = min(cursor.plus(window.getWindowUsed()), endLimit);
                    LogHelper.info(ctx, RunPhase.WINDOW, "empty rows, queriesExecuted=" + queriesExecuted + ", rowsProcessed=" + rowsProcessed + ", cursor=" + cursor);
                    continue;
                }

                List<PreparedRecord> preparedRecords = transformRecords(ctx, entity, window);
                rowsProcessed += preparedRecords.size();

                Instant maxTimestampRows = resolveMaxTimestamp(window, preparedRecords);

                if (!preparedRecords.isEmpty()) {
                    writeBulkInChunks(preparedRecords, ctx, entity);
                } else {
                    LogHelper.warn(ctx, RunPhase.CHECKPOINT, "no transformed rows available for checkpoint update");
                }

                cursor = advanceCursor(cursor, maxTimestampRows, endLimit);
                LogHelper.info(ctx, RunPhase.WINDOW, "queriesExecuted=" + queriesExecuted + ", rowsProcessed=" + rowsProcessed + ", cursor=" + cursor + ", maxTimestampRows=" + maxTimestampRows);
            }

            LogHelper.info(ctx, RunPhase.END, "queriesExecuted=" + queriesExecuted + ", rowsProcessed=" + rowsProcessed);

        } catch (Exception e) {
            LogHelper.error(ctx, RunPhase.ERROR, "Unhandled exception: " + e.getMessage());
            throw new RuntimeException(e);
        } finally {
            LogHelper.info(ctx, RunPhase.END, "run completed");
        }
    }

    private Instant determineCursor(RunContext ctx, EntityName entity, Instant endLimit) {
        Optional<Instant> checkpointOpt = checkpointStore.getCheckpoint(entity);

        if (checkpointOpt.isPresent()) {
            Instant checkpoint = checkpointOpt.get();
            LogHelper.info(ctx, RunPhase.CHECKPOINT, "cursor set from checkpoint: " + checkpoint);
            return checkpoint;
        }

        Optional<Instant> oldestOpt = oldestTimestampProvider.getOldestTimestamp(ctx, entity);
        if (oldestOpt.isPresent()) {
            Instant oldest = oldestOpt.get();
            LogHelper.info(ctx, RunPhase.CHECKPOINT, "cursor set from oldest timestamp: " + oldest);
            return oldest;
        }

        Instant bootstrapCursor = endLimit.minus(ingestionConfig.getInitialWindow());
        LogHelper.warn(ctx, RunPhase.CHECKPOINT,
                "No checkpoint or oldest timestamp found, using bootstrap cursor=" + bootstrapCursor);
        return bootstrapCursor;
    }

    private List<PreparedRecord> transformRecords(RunContext ctx, EntityName entity, AdxWindowResult window) {
        List<PreparedRecord> preparedRecords = new ArrayList<>();
        for (Map.Entry<String, Object> entry : getDeterministicRows(window.getRows())) {
            Object value = entry.getValue();
            if (!(value instanceof Map<?, ?> rawRow)) {
                LogHelper.error(ctx, RunPhase.ERROR, "Unexpected row payload type for key=" + entry.getKey());
                Map<String, Object> fallbackPayload = new java.util.HashMap<>();
                fallbackPayload.put("raw", value);
                stagingErrorService.insertError(
                        ctx,
                        entry.getKey(),
                        fallbackPayload,
                        new IllegalArgumentException("Unexpected row payload type")
                );
                continue;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rawRow;

            try {
                Object transformed = entityTransformer.transform(row, getTargetClass(entity));
                preparedRecords.add(new PreparedRecord(transformed, row, extractInsertedTimestamp(row).orElse(window.getToExclusive()), entry.getKey()));
            } catch (EntityTransformer.TransformationException e) {
                LogHelper.error(ctx, RunPhase.ERROR, "Transformation failed for row key=" + entry.getKey() + ": " + e.getMessage());
                stagingErrorService.insertError(ctx, entry.getKey(), row, e);
            }
        }
        return preparedRecords;
    }

    private void writeBulkInChunks(List<PreparedRecord> records, RunContext ctx, EntityName entity) {
        int bulkSize = Math.max(1, ingestionConfig.getBulkInsertSize());
        String runId = ctx.getRunId();

        for (int i = 0; i < records.size(); i += bulkSize) {
            int end = Math.min(i + bulkSize, records.size());
            List<PreparedRecord> chunk = records.subList(i, end);
            List<Object> payload = chunk.stream()
                    .map(PreparedRecord::transformedRecord)
                    .collect(Collectors.toList());

            try {
                BulkWriteResult result = bulkWriter.writeBulk(entity, payload);
                Instant checkpointTs = chunk.stream()
                        .map(PreparedRecord::insertedTimestamp)
                        .filter(Objects::nonNull)
                        .max(Comparator.naturalOrder())
                        .orElse(result.getMaxInsertedTimestamp());

                checkpointStore.updateCheckpoint(
                        entity,
                        checkpointTs,
                        runId
                );

                LogHelper.info(ctx, RunPhase.CHECKPOINT, "bulk write completed: rowsInserted=" + result.getRowsInserted() + ", checkpointTs=" + checkpointTs);

            } catch (BulkWriter.BulkWriteException e) {
                LogHelper.error(ctx, RunPhase.ERROR, "Bulk write failed: " + e.getMessage());
                for (PreparedRecord preparedRecord : chunk) {
                    stagingErrorService.insertError(ctx, preparedRecord.sourceKey(), preparedRecord.sourceRow(), e);
                }
            }
        }
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

    private Instant resolveMaxTimestamp(AdxWindowResult window, List<PreparedRecord> preparedRecords) {
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
}
