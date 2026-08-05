package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Cleans up old rows from INGEST_EXECUTION_LOG. Invoked by a single Quartz job (clustered,
 * fires once across all pods) rather than {@code @Scheduled} (which would fire on every replica).
 * Deletes are performed in bounded batches, each in its own transaction, to avoid a single
 * long-running DELETE that holds locks and a pooled connection.
 */
@Slf4j
@Service
public class ExecutionLogCleanupService {

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;
    private final TransactionTemplate transactionTemplate;

    @Value("${ingestion.executionLog.enabled:true}")
    private boolean enabled;

    @Value("${ingestion.executionLog.retentionDays:2}")
    private int retentionDays;

    @Value("${ingestion.executionLog.batchSize:500}")
    private int batchSize;

    public ExecutionLogCleanupService(JdbcTemplate jdbcTemplate,
                                      DbSchemaConfig dbSchemaConfig,
                                      PlatformTransactionManager transactionManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbSchemaConfig = dbSchemaConfig;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public long cleanup(String runId) {
        if (!enabled) {
            log.info("[runId={}][entityName=EXECUTION_LOG_CLEANUP][phase=NOOP] cleanup disabled", runId);
            return 0;
        }

        OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minusDays(retentionDays);
        int limit = Math.max(1, batchSize);
        String table = dbSchemaConfig.getSchemaName() + ".INGEST_EXECUTION_LOG";
        String sql = "DELETE FROM " + table + " WHERE ID IN "
                + "(SELECT ID FROM " + table + " WHERE CREATED_AT < ? ORDER BY ID LIMIT ?)";

        long totalDeleted = 0;
        int deleted;
        do {
            Integer batch = transactionTemplate.execute(status ->
                    jdbcTemplate.update(sql, threshold, limit));
            deleted = batch != null ? batch : 0;
            totalDeleted += deleted;
        } while (deleted == limit);

        log.info("[runId={}][entityName=EXECUTION_LOG_CLEANUP][phase=END] retentionDays={} threshold={} deleted={}",
                runId, retentionDays, threshold, totalDeleted);
        return totalDeleted;
    }
}
