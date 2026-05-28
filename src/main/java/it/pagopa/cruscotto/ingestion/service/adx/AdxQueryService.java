package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class AdxQueryService {
    private final AdxClient adxClient;
    private final IngestionConfig ingestionConfig;
    private final PositionAdxQueryBuilder positionBuilder;
    private final PositionTokensAdxQueryBuilder positionTokensBuilder;
    private final TransfersAdxQueryBuilder transfersBuilder;
    private final EventsWfAdxQueryBuilder eventsWfBuilder;
    private final ExtraInfoAdxQueryBuilder extraInfoBuilder;

    public Optional<AdxWindowResult> fetchWindow(
            RunContext ctx,
            Instant cursor,
            Duration window,
            Instant endLimit) {

        String runId = ctx.getRunId();
        String entityName = ctx.getEntityName();
        String operationId = ctx.getOperationId();

        Duration currentWindow = window;
        int attempt = 0;

        while (attempt < ingestionConfig.getMaxWindowHalvingAttempts()) {
            attempt++;

            Instant to = cursor.plus(currentWindow);
            if (to.isAfter(endLimit)) {
                to = endLimit;
            }

            log.info("WINDOW runId={} operationId={} entityName={} cursor={} to={} window={} attempt={}",
                    runId, operationId, entityName, cursor, to, currentWindow, attempt);

            AdxQueryResult result = executeEntityQuery(ctx, cursor, to);

            if (result.isSuccess()) {
                int extractedRows = countExtractedRows(result.getData());
                log.info("WINDOW_SUCCESS runId={} operationId={} entityName={} cursor={} to={} window={} attempt={}",
                        runId, operationId, entityName, cursor, to, currentWindow, attempt);
                log.info("ADX_RESULT runId={} operationId={} entityName={} extractedRows={} attempt={} window={}",
                        runId, operationId, entityName, extractedRows, attempt, currentWindow);
                if (log.isDebugEnabled()) {
                    log.debug("ADX_RESULT_DEBUG runId={} operationId={} entityName={} sampleKey={} sampleRow={}",
                            runId, operationId, entityName, sampleKey(result.getData()), sampleRow(result.getData()));
                }

                return Optional.of(new AdxWindowResult(
                        cursor,
                        to,
                        currentWindow,
                        attempt,
                        result.getData() != null ? result.getData() : new HashMap<>()
                ));
            }

            // Check if error is result-set-too-large
            if (isResultSetTooLargeError(result.getError())) {
                log.warn("RESULT_SET_TOO_LARGE runId={} operationId={} entityName={} cursor={} to={} window={} attempt={}",
                        runId, operationId, entityName, cursor, to, currentWindow, attempt);

                // Halve the window and retry
                currentWindow = currentWindow.dividedBy(2);
                continue;
            }

            // Other errors: fail immediately
            log.error("QUERY_ERROR runId={} operationId={} entityName={} cursor={} to={} window={} attempt={} error={}",
                    runId, operationId, entityName, cursor, to, currentWindow, attempt, result.getError());
            return Optional.empty();
        }

        // Max attempts exceeded
        log.error("WINDOW_TOO_LARGE runId={} operationId={} entityName={} cursor={} window={} maxAttempts={}",
                runId, operationId, entityName, cursor, currentWindow, ingestionConfig.getMaxWindowHalvingAttempts());

        throw new AdxWindowTooLargeException(runId, entityName, cursor, currentWindow);
    }

    private boolean isResultSetTooLargeError(String error) {
        if (error == null) {
            return false;
        }

        String maxSize = String.valueOf(ingestionConfig.getAdx().getMaxResultSizeMb());
        return error.contains("LimitsExceeded")
                || error.contains("E_QUERY_RESULT_SET_TOO_LARGE")
                || error.contains(maxSize + "MB")
                || error.contains(maxSize + " MB");
    }

    private String buildQuery(RunContext ctx, Instant cursor, Instant to) {
        EntityName entity = EntityName.valueOf(ctx.getEntityName());

        return switch (entity) {
            case POSITION -> positionBuilder.buildQuery(ctx, cursor, to);
            case POSITION_TOKENS -> positionTokensBuilder.buildQuery(ctx, cursor, to);
            case POSITION_TRANSFERS -> transfersBuilder.buildQuery(ctx, cursor, to);
            case EVENTS_WF -> eventsWfBuilder.buildQuery(ctx, cursor, to);
            case EXTRA_INFO -> extraInfoBuilder.buildQuery(ctx, cursor, to);
            default -> throw new IllegalArgumentException("No query builder configured for entity: " + entity);
        };
    }

    private AdxQueryResult executeEntityQuery(RunContext ctx, Instant cursor, Instant to) {
        EntityName entity = EntityName.valueOf(ctx.getEntityName());
        if (entity == EntityName.EVENTS_WF) {
            return executeEventsWfQueries(ctx, cursor, to);
        }

        String query = buildQuery(ctx, cursor, to);
        log.info("ADX_QUERY runId={} operationId={} entityName={} queryLength={} queryHash={}",
                ctx.getRunId(), ctx.getOperationId(), ctx.getEntityName(), query.length(), Integer.toHexString(query.hashCode()));
        return adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), query);
    }

    private AdxQueryResult executeEventsWfQueries(RunContext ctx, Instant cursor, Instant to) {
        String reqRespQuery = eventsWfBuilder.buildReqRespQuery(ctx, cursor, to);
        log.info("ADX_QUERY runId={} operationId={} entityName={} type=REQ_RESP queryLength={} queryHash={}",
                ctx.getRunId(), ctx.getOperationId(), ctx.getEntityName(), reqRespQuery.length(), Integer.toHexString(reqRespQuery.hashCode()));
        AdxQueryResult reqRespResult = adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), reqRespQuery);
        if (!reqRespResult.isSuccess()) {
            return new AdxQueryResult(false, null, reqRespResult.getError());
        }

        String receiptQuery = eventsWfBuilder.buildReceiptQuery(ctx, cursor, to);
        log.info("ADX_QUERY runId={} operationId={} entityName={} type=RECEIPT queryLength={} queryHash={}",
                ctx.getRunId(), ctx.getOperationId(), ctx.getEntityName(), receiptQuery.length(), Integer.toHexString(receiptQuery.hashCode()));
        AdxQueryResult receiptResult = adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), receiptQuery);
        if (!receiptResult.isSuccess()) {
            return new AdxQueryResult(false, null, receiptResult.getError());
        }

        Map<String, Object> mergedRows = mergeRows(reqRespResult.getData(), receiptResult.getData());
        log.info("ADX_RESULT runId={} operationId={} entityName={} reqRespRows={} receiptRows={} mergedRows={}",
                ctx.getRunId(), ctx.getOperationId(), ctx.getEntityName(), countExtractedRows(reqRespResult.getData()),
                countExtractedRows(receiptResult.getData()), mergedRows.size());
        return new AdxQueryResult(true, mergedRows, null);
    }

    private Map<String, Object> mergeRows(Map<String, Object> first, Map<String, Object> second) {
        Map<String, Object> merged = new LinkedHashMap<>();
        appendRows(merged, first);
        appendRows(merged, second);
        return merged;
    }

    private void appendRows(Map<String, Object> target, Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            String uniqueKey = key;
            int suffix = 1;
            while (target.containsKey(uniqueKey)) {
                uniqueKey = key + "#" + suffix;
                suffix++;
            }
            target.put(uniqueKey, entry.getValue());
        }
    }

    private int countExtractedRows(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return 0;
        }
        return data.size();
    }

    private String sampleKey(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return "n/a";
        }
        return data.keySet().iterator().next();
    }

    private Object sampleRow(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return "n/a";
        }
        return data.values().iterator().next();
    }
}

