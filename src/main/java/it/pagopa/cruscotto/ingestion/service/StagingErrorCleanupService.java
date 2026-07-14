package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Slf4j
@Service
@RequiredArgsConstructor
public class StagingErrorCleanupService {

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;
    private final IngestionConfig ingestionConfig;

    @Transactional
    public int cleanup(String runId) {
        IngestionConfig.StagingErrorCleanupConfig config = ingestionConfig.getStagingErrorCleanup();
        if (config == null || !config.isEnabled()) {
            log.info("[runId={}][entityName=STG_INGEST_ERROR][phase=NOOP] cleanup disabled", runId);
            return 0;
        }

        OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minus(config.getRetention());
        String sql = "DELETE FROM " + dbSchemaConfig.getSchemaName() + ".STG_INGEST_ERROR WHERE CREATED_AT < ?";
        int deleted = jdbcTemplate.update(sql, threshold);
        log.info("[runId={}][entityName=STG_INGEST_ERROR][phase=END] retention={} threshold={} deleted={}",
                runId, config.getRetention(), threshold, deleted);
        return deleted;
    }
}
