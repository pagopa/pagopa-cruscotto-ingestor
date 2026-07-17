package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Resolves canonical token IDs through POSITION_TOKEN_REGISTRY in batches.
 * Unresolved tokens deliberately remain absent from the prefetch cache so the
 * individual resolver retains its existing registry-miss fallback.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TokenFkBatchPrefetcher {

    private static final int BATCH_SIZE = 500;

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;

    public void prefetchForPositionTransfers(Map<String, Object> windowRows,
                                             BatchLocalCache cache,
                                             RunContext ctx) {
        if (windowRows == null || windowRows.isEmpty() || cache == null) {
            return;
        }

        Map<String, byte[]> uniqueTokens = new LinkedHashMap<>();
        for (Object rowValue : windowRows.values()) {
            if (!(rowValue instanceof Map<?, ?> rawRow)) {
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rawRow;
            byte[] token = tokenValue(row);
            if (token == null) {
                continue;
            }
            String tokenBase64 = Base64.getEncoder().encodeToString(token);
            if (!cache.hasTokenCanonicalLookupResult(tokenBase64)) {
                uniqueTokens.putIfAbsent(tokenBase64, token);
            }
        }

        List<Map.Entry<String, byte[]>> tokens = new ArrayList<>(uniqueTokens.entrySet());
        for (int start = 0; start < tokens.size(); start += BATCH_SIZE) {
            prefetchChunk(tokens.subList(start, Math.min(start + BATCH_SIZE, tokens.size())), cache, ctx);
        }
    }

    private void prefetchChunk(List<Map.Entry<String, byte[]>> tokens, BatchLocalCache cache, RunContext ctx) {
        StringJoiner values = new StringJoiner(", ");
        List<Object> parameters = new ArrayList<>(tokens.size() * 2);
        for (int index = 0; index < tokens.size(); index++) {
            values.add("(?::integer, ?::bytea)");
            parameters.add(index);
            parameters.add(tokens.get(index).getValue());
        }

        String schema = dbSchemaConfig.getSchemaName();
        String sql = "WITH requested(request_key, token) AS (VALUES " + values + ") "
                + "SELECT r.request_key, p.id AS token_id, p.fk_position AS fk_position "
                + "FROM requested r "
                + "LEFT JOIN LATERAL ("
                + " SELECT position_token.id, position_token.FK_POSITION AS fk_position FROM " + schema + ".POSITION_TOKEN_REGISTRY registry "
                + " JOIN " + schema + ".POSITION_TOKENS position_token "
                + "   ON position_token.TOKEN = registry.TOKEN "
                + "  AND position_token.DATE_EVENT = registry.FIRST_DATE_EVENT "
                + " WHERE registry.TOKEN = r.token "
                + " ORDER BY position_token.ID ASC "
                + " LIMIT 1"
                + ") p ON TRUE";

        try {
            jdbcTemplate.query(sql, rs -> {
                int requestedIndex = rs.getInt("request_key");
                Integer tokenId = (Integer) rs.getObject("token_id");
                if (tokenId != null) {
                    Integer fkPosition = (Integer) rs.getObject("fk_position");
                    cache.putTokenWindowPrefetch(tokens.get(requestedIndex).getKey(), tokenId);
                    cache.cacheTokenCanonicalLookupResult(tokens.get(requestedIndex).getKey(), tokenId);
                    cache.cacheTokenCanonicalFkPosition(tokens.get(requestedIndex).getKey(), fkPosition);
                }
            }, parameters.toArray());
        } catch (Exception exception) {
            log.warn("[{}] Token FK prefetch failed for {} tokens: {}",
                    ctx != null ? ctx.getRunId() : "?", tokens.size(), exception.getMessage());
        }
    }

    private byte[] tokenValue(Map<String, Object> row) {
        Object value = row.get("TOKEN");
        if (value == null) {
            value = row.get("token");
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof String string) {
            return string.isBlank() ? null : string.getBytes(StandardCharsets.UTF_8);
        }
        return value != null ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : null;
    }
}
