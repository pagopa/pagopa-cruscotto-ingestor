package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Prunes Spring Batch metadata tables (BATCH_JOB_EXECUTION and children) older than the
 * configured retention. Without pruning these tables grow unbounded (every job run adds rows),
 * which after weeks slows down JobRepository queries and job launching.
 * <p>
 * Deletes are performed in batches of old JOB_EXECUTION ids, each batch in its own transaction,
 * so a large backlog can never turn into a single long-running DELETE holding locks or a
 * connection for minutes. Child tables are deleted before parents to respect foreign keys.
 */
@Slf4j
@Service
public class BatchMetadataCleanupService {

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;
    private final IngestionConfig ingestionConfig;
    private final TransactionTemplate transactionTemplate;

    public BatchMetadataCleanupService(JdbcTemplate jdbcTemplate,
                                       DbSchemaConfig dbSchemaConfig,
                                       IngestionConfig ingestionConfig,
                                       PlatformTransactionManager transactionManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbSchemaConfig = dbSchemaConfig;
        this.ingestionConfig = ingestionConfig;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public int cleanup(String runId) {
        IngestionConfig.BatchMetadataCleanupConfig config = ingestionConfig.getBatchMetadataCleanup();
        if (config == null || !config.isEnabled()) {
            try (MdcScope ignored = MdcScope.run(runId, "BATCH_METADATA_PURGE", "NOOP")) {
                log.info("Skipped cleanup, disabled by configuration");
            }
            return 0;
        }

        Instant threshold = Instant.now().minus(config.getRetention());
        int batchSize = Math.max(1, config.getBatchSize());
        Timestamp thresholdTs = Timestamp.from(threshold);

        int totalDeleted = 0;
        int batchDeleted;
        do {
            final int limit = batchSize;
            Integer deleted = transactionTemplate.execute(status -> deleteBatch(thresholdTs, limit));
            batchDeleted = deleted != null ? deleted : 0;
            totalDeleted += batchDeleted;
        } while (batchDeleted == batchSize);

        Integer orphans = transactionTemplate.execute(status -> deleteOrphanJobInstances());
        int orphanInstances = orphans != null ? orphans : 0;

        try (MdcScope ignored = MdcScope.run(runId, "BATCH_METADATA_PURGE", "END");
             LogDetails details = LogDetails.open()
                     .with(LogFields.DEPENDENCY, "postgres")
                     .with("retention", config.getRetention())
                     .with("threshold", threshold)
                     .with("deleted_job_executions", totalDeleted)
                     .with("deleted_orphan_instances", orphanInstances)) {
            log.info("Completed batch metadata cleanup");
        }
        return totalDeleted;
    }

    /**
     * Deletes one batch of the oldest JOB_EXECUTION rows (and their child rows) older than the
     * threshold. Runs inside the caller's transaction. Returns the number of JOB_EXECUTION rows deleted.
     */
    private int deleteBatch(Timestamp thresholdTs, int batchSize) {
        String prefix = batchTablePrefix();

        List<Long> jobExecutionIds = jdbcTemplate.queryForList(
                "SELECT JOB_EXECUTION_ID FROM " + prefix + "JOB_EXECUTION "
                        + "WHERE CREATE_TIME < ? ORDER BY JOB_EXECUTION_ID LIMIT ?",
                Long.class, thresholdTs, batchSize);

        if (jobExecutionIds.isEmpty()) {
            return 0;
        }

        String inClause = buildInClause(jobExecutionIds.size());
        Object[] ids = jobExecutionIds.toArray();

        jdbcTemplate.update(
                "DELETE FROM " + prefix + "STEP_EXECUTION_CONTEXT WHERE STEP_EXECUTION_ID IN "
                        + "(SELECT STEP_EXECUTION_ID FROM " + prefix + "STEP_EXECUTION "
                        + "WHERE JOB_EXECUTION_ID IN " + inClause + ")",
                ids);
        jdbcTemplate.update(
                "DELETE FROM " + prefix + "STEP_EXECUTION WHERE JOB_EXECUTION_ID IN " + inClause, ids);
        jdbcTemplate.update(
                "DELETE FROM " + prefix + "JOB_EXECUTION_CONTEXT WHERE JOB_EXECUTION_ID IN " + inClause, ids);
        jdbcTemplate.update(
                "DELETE FROM " + prefix + "JOB_EXECUTION_PARAMS WHERE JOB_EXECUTION_ID IN " + inClause, ids);
        jdbcTemplate.update(
                "DELETE FROM " + prefix + "JOB_EXECUTION WHERE JOB_EXECUTION_ID IN " + inClause, ids);

        return jobExecutionIds.size();
    }

    /**
     * Removes JOB_INSTANCE rows that no longer have any JOB_EXECUTION referencing them.
     */
    private int deleteOrphanJobInstances() {
        String prefix = batchTablePrefix();
        return jdbcTemplate.update(
                "DELETE FROM " + prefix + "JOB_INSTANCE WHERE JOB_INSTANCE_ID NOT IN "
                        + "(SELECT DISTINCT JOB_INSTANCE_ID FROM " + prefix + "JOB_EXECUTION)");
    }

    private String batchTablePrefix() {
        return dbSchemaConfig.getSchemaName() + ".BATCH_";
    }

    private String buildInClause(int size) {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < size; i++) {
            sb.append(i == 0 ? "?" : ",?");
        }
        return sb.append(")").toString();
    }
}
