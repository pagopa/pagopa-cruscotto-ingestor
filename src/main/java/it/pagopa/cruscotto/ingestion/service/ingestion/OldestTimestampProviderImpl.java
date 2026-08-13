package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import it.pagopa.cruscotto.ingestion.service.adx.AdxClient;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryResult;
import it.pagopa.cruscotto.ingestion.service.adx.AdxTimestamps;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OldestTimestampProviderImpl implements OldestTimestampProvider {
    private static final String OLDEST_TIMESTAMP = "OLDEST_TIMESTAMP";

    private final AdxClient adxClient;
    private final IngestionConfig ingestionConfig;
    private final AdxTableNamesConfig tableNamesConfig;

    @Override
    public Optional<Instant> getOldestTimestamp(RunContext ctx, EntityName entity) {
        String source = resolveAdxSource(entity);
        if (source == null) {
            LogHelper.warn(ctx, RunPhase.CHECKPOINT, "no ADX oldest-timestamp source configured for entity=" + entity.name());
            return Optional.empty();
        }

        String query = source + "\n| summarize OLDEST_TIMESTAMP=min(INSERTED_TIMESTAMP)";
        log.debug("ADX_OLDEST_TIMESTAMP_QUERY runId={} entityName={} source={} query={}",
                ctx.getRunId(), ctx.getEntityName(), source, query);

        AdxQueryResult result = adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), query);
        if (!result.isSuccess()) {
            LogHelper.warn(ctx, RunPhase.CHECKPOINT,
                    "failed to resolve oldest timestamp from ADX source=" + source + ", error=" + result.getError());
            return Optional.empty();
        }

        Optional<Instant> oldest = extractOldestTimestamp(result.getData());
        if (oldest.isPresent()) {
            Instant oldestTimestamp = oldest.orElseThrow(
                    () -> new IllegalStateException("Oldest timestamp unexpectedly absent for source=" + source));
            LogHelper.info(ctx, RunPhase.CHECKPOINT,
                    "oldest timestamp resolved from ADX source=" + source + ": " + oldestTimestamp);
            return oldest;
        }

        LogHelper.warn(ctx, RunPhase.CHECKPOINT,
                "ADX returned no oldest timestamp for source=" + source + ", falling back to bootstrap cursor");
        if (result.getData() != null && !result.getData().isEmpty()) {
            Object firstRow = result.getData().values().iterator().next();
            log.warn("ADX_OLDEST_TIMESTAMP_UNRESOLVED runId={} entityName={} source={} row={}",
                    ctx.getRunId(), ctx.getEntityName(), source, firstRow);
        }
        return Optional.empty();
    }

    private String resolveAdxSource(EntityName entity) {
        return switch (entity) {
            case POSITION, POSITION_TOKENS, POSITION_TRANSFERS, EVENTS_WF, EXTRA_INFO ->
                    tableNamesConfig.getTableName(entity.name());
            default -> null;
        };
    }

    private Optional<Instant> extractOldestTimestamp(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return Optional.empty();
        }

        Object firstRow = data.values().iterator().next();
        if (!(firstRow instanceof Map<?, ?> rawRow)) {
            return Optional.empty();
        }

        return toInstant(getOldestTimestampValue(rawRow));
    }

    private Object getOldestTimestampValue(Map<?, ?> row) {
        for (Map.Entry<?, ?> entry : row.entrySet()) {
            Object key = entry.getKey();
            if (key != null && OLDEST_TIMESTAMP.equalsIgnoreCase(String.valueOf(key))) {
                return entry.getValue();
            }
        }
        return null;
    }

    private Optional<Instant> toInstant(Object value) {
        return AdxTimestamps.toInstant(value);
    }
}

