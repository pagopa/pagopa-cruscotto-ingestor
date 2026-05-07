package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import it.pagopa.cruscotto.ingestion.service.adx.AdxClient;
import it.pagopa.cruscotto.ingestion.service.adx.AdxQueryResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OldestTimestampProviderImpl implements OldestTimestampProvider {
    private static final String OLDEST_TIMESTAMP = "OLDEST_TIMESTAMP";

    private final AdxClient adxClient;
    private final IngestionConfig ingestionConfig;

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
            LogHelper.info(ctx, RunPhase.CHECKPOINT,
                    "oldest timestamp resolved from ADX source=" + source + ": " + oldest.get());
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
            case POSITION -> "SERT_POSITION";
            case POSITION_TOKENS -> "SERT_POSITION_TOKENS";
            case POSITION_TRANSFERS -> "SERT_TRANSFERS";
            case EVENTS_WF -> "SERT_EVENTS_WF";
            case EXTRA_INFO -> "EXTRA_INFO";
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
        if (value instanceof Timestamp timestamp) {
            return Optional.of(timestamp.toInstant());
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
                        try {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy, HH:mm:ss,SSSSSS", Locale.ITALY);
                            return Optional.of(LocalDateTime.parse(timestamp, formatter).toInstant(ZoneOffset.UTC));
                        } catch (DateTimeParseException ignoredFourth) {
                            return Optional.empty();
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }
}

