package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Resolves POSITION foreign keys for one ADX window with the same 24-hour
 * matching rule used by the individual resolver.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PositionFkBatchPrefetcher {

    private static final int BATCH_SIZE = 500;

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;

    public void prefetchForPositionTokens(Map<String, Object> windowRows,
                                          BatchLocalCache cache,
                                          RunContext ctx) {
        if (windowRows == null || windowRows.isEmpty() || cache == null) {
            return;
        }

        Map<String, PositionKey> uniqueKeys = new LinkedHashMap<>();
        for (Object rowValue : windowRows.values()) {
            if (!(rowValue instanceof Map<?, ?> rawRow)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rawRow;
            String nav = stringValue(row, "NAV", "nav");
            String paEmittente = stringValue(row, "PA_EMITTENTE", "pa_emittente", "paEmittente");
            LocalDateTime sourceTimestamp = timestampValue(row,
                    "INSERTED_TIMESTAMP_RESP", "inserted_timestamp_resp", "insertedTimestampResp",
                    "INSERTED_TIMESTAMP_REQ", "inserted_timestamp_req", "insertedTimestampReq",
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp");
            if (nav == null || paEmittente == null || sourceTimestamp == null) {
                continue;
            }
            PositionKey key = new PositionKey(nav, paEmittente, sourceTimestamp);
            uniqueKeys.putIfAbsent(key.cacheKey(), key);
        }

        List<PositionKey> keys = new ArrayList<>(uniqueKeys.values());
        for (int start = 0; start < keys.size(); start += BATCH_SIZE) {
            prefetchChunk(keys.subList(start, Math.min(start + BATCH_SIZE, keys.size())), cache, ctx);
        }
    }

    private void prefetchChunk(List<PositionKey> keys, BatchLocalCache cache, RunContext ctx) {
        StringJoiner values = new StringJoiner(", ");
        List<Object> parameters = new ArrayList<>(keys.size() * 4);
        for (int index = 0; index < keys.size(); index++) {
            PositionKey key = keys.get(index);
            values.add("(?, ?, ?, ?)");
            parameters.add(index);
            parameters.add(key.nav());
            parameters.add(key.paEmittente());
            parameters.add(Timestamp.valueOf(key.sourceTimestamp()));
        }

        String schema = dbSchemaConfig.getSchemaName();
        String sql = "WITH requested(request_key, nav, pa_emittente, source_timestamp) AS (VALUES " + values + ") "
                + "SELECT r.request_key, p.id AS position_id "
                + "FROM requested r "
                + "LEFT JOIN LATERAL ("
                + " SELECT position.id FROM " + schema + ".POSITION position "
                + " WHERE position.NAV = r.nav "
                + "   AND position.PA_EMITTENTE = r.pa_emittente "
                + "   AND position.DATE_EVENT BETWEEN (r.source_timestamp - INTERVAL '24 hours')::date "
                + "       AND r.source_timestamp::date "
                + "   AND position.INSERTED_TIMESTAMP BETWEEN r.source_timestamp - INTERVAL '24 hours' "
                + "       AND r.source_timestamp "
                + " ORDER BY position.INSERTED_TIMESTAMP DESC, position.ID DESC "
                + " LIMIT 1"
                + ") p ON TRUE";

        try {
            jdbcTemplate.query(sql, rs -> {
                int requestedIndex = rs.getInt("request_key");
                int positionId = rs.getInt("position_id");
                if (!rs.wasNull()) {
                    PositionKey key = keys.get(requestedIndex);
                    cache.putPositionWindowPrefetch(key.nav(), key.paEmittente(), key.sourceTimestamp(), positionId);
                }
            }, parameters.toArray());
        } catch (Exception exception) {
            log.warn("[{}] Position FK prefetch failed for {} keys: {}",
                    ctx != null ? ctx.getRunId() : "?", keys.size(), exception.getMessage());
        }
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

    private LocalDateTime timestampValue(Map<String, Object> row, String... keys) {
        for (String key : keys) {
            Object value = row.get(key);
            LocalDateTime converted = toLocalDateTime(value);
            if (converted != null) {
                return converted;
            }
        }
        return null;
    }

    private LocalDateTime toLocalDateTime(Object value) {
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime;
        }
        if (value instanceof Instant instant) {
            return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toLocalDateTime();
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return zonedDateTime.toLocalDateTime();
        }
        if (value instanceof java.util.Date date) {
            return LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC);
        }
        if (value instanceof CharSequence sequence) {
            try {
                return LocalDateTime.parse(sequence);
            } catch (Exception ignored) {
                try {
                    return LocalDateTime.ofInstant(Instant.parse(sequence), ZoneOffset.UTC);
                } catch (Exception ignoredAgain) {
                    return null;
                }
            }
        }
        return null;
    }

    private record PositionKey(String nav, String paEmittente, LocalDateTime sourceTimestamp) {
        private String cacheKey() {
            return nav + "\u0000" + paEmittente + "\u0000" + sourceTimestamp;
        }
    }
}
