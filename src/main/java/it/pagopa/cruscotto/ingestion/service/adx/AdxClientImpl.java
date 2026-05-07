package it.pagopa.cruscotto.ingestion.service.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AdxClientImpl implements AdxClient {
    private static final String UNIQUE_ID = "UNIQUE_ID";
    private static final String FALLBACK_COLUMN_PREFIX = "col_";

    private final Client kustoClient;
    private final IngestionConfig ingestionConfig;

    @Override
    public AdxQueryResult executeQuery(RunContext ctx, String database, String query) {
        String runId = ctx.getRunId();
        String entityName = ctx.getEntityName();

        if (database == null || database.isBlank()) {
            return new AdxQueryResult(false, null, "Invalid ADX database: value is blank");
        }
        if (query == null || query.isBlank()) {
            return new AdxQueryResult(false, null, "Invalid ADX query: value is blank");
        }

        long startedAt = System.currentTimeMillis();
        try {
            ClientRequestProperties requestProperties = buildRequestProperties();
            KustoOperationResult operationResult = kustoClient.execute(database, query, requestProperties);
            KustoResultSetTable table = operationResult == null ? null : operationResult.getPrimaryResults();
            Map<String, Object> rows = mapRows(table);
            log.info("ADX_CLIENT_SUCCESS runId={} entityName={} database={} rows={} elapsedMs={}",
                    runId, entityName, database, rows.size(), System.currentTimeMillis() - startedAt);
            return new AdxQueryResult(true, rows, null);
        } catch (Exception e) {
            String error = buildErrorMessage(e);
            log.error("ADX_QUERY_EXECUTION_ERROR runId={} entityName={} database={} error={} elapsedMs={}",
                    runId, entityName, database, error, System.currentTimeMillis() - startedAt, e);
            return new AdxQueryResult(false, null, error);
        }
    }

    private ClientRequestProperties buildRequestProperties() {
        ClientRequestProperties requestProperties = new ClientRequestProperties();
        requestProperties.setApplication("cruscotto-ingestor");
        requestProperties.setClientRequestId("cruscotto-ingestor;" + UUID.randomUUID());
        Duration queryTimeout = ingestionConfig.getAdx().getQueryTimeout();
        if (queryTimeout != null) {
            requestProperties.setTimeoutInMilliSec(queryTimeout.toMillis());
        }
        return requestProperties;
    }

    private Map<String, Object> mapRows(KustoResultSetTable table) {
        Map<String, Object> rows = new LinkedHashMap<>();
        if (table == null) {
            return rows;
        }

        KustoResultColumn[] tableColumns = table.getColumns();
        List<KustoResultColumn> columns = tableColumns == null ? List.of() : List.of(tableColumns);
        if (log.isDebugEnabled()) {
            log.debug("ADX_RESULT_COLUMNS columns={}", columns.stream().map(this::resolveColumnName).toList());
        }
        Set<String> usedKeys = new HashSet<>();
        int rowIndex = 0;
        while (table.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (KustoResultColumn column : columns) {
                String columnName = resolveColumnName(column);
                row.put(columnName, normalizeValue(table.getObject(column.getOrdinal())));
            }

            String key = ensureUniqueKey(resolveRowKey(row, rowIndex), usedKeys);
            rows.put(key, row);
            if (rowIndex == 0 && log.isDebugEnabled()) {
                log.debug("ADX_RESULT_SAMPLE key={} row={}", key, row);
            }
            rowIndex++;
        }
        return rows;
    }

    private String resolveColumnName(KustoResultColumn column) {
        String name = column.getColumnName();
        if (name == null || name.isBlank()) {
            return FALLBACK_COLUMN_PREFIX + column.getOrdinal();
        }
        return name;
    }

    private String resolveRowKey(Map<String, Object> row, int rowIndex) {
        Object uniqueId = row.get(UNIQUE_ID);
        if (uniqueId != null) {
            String value = String.valueOf(uniqueId).trim();
            if (!value.isEmpty()) {
                return value;
            }
        }
        return "row_" + rowIndex;
    }

    private String ensureUniqueKey(String candidate, Set<String> usedKeys) {
        String key = candidate;
        int suffix = 1;
        while (usedKeys.contains(key)) {
            key = candidate + "#" + suffix;
            suffix++;
        }
        usedKeys.add(key);
        return key;
    }

    private String buildErrorMessage(Exception e) {
        String message = e.getMessage();
        if (message == null || message.isBlank()) {
            message = "no message";
        }
        return e.getClass().getSimpleName() + ": " + message;
    }

    private Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Instant) {
            return value;
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return zonedDateTime.toInstant();
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime.toInstant(ZoneOffset.UTC);
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant();
        }
        return value;
    }
}

