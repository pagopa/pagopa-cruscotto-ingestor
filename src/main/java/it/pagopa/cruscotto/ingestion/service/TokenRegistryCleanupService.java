package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;

@Slf4j
@Service
@RequiredArgsConstructor
public class TokenRegistryCleanupService {

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;
    private final IngestionConfig ingestionConfig;

    @Transactional
    public int cleanup(String runId) {
        IngestionConfig.TokenRegistryCleanupConfig config = ingestionConfig.getTokenRegistryCleanup();
        if (config == null || !config.isEnabled()) {
            log.info("[runId={}][entityName=TOKEN_REGISTRY_PURGE][phase=NOOP] cleanup disabled", runId);
            return 0;
        }

        LocalDate cutoff = LocalDate.now().minus(config.getRetention());
        String sql = "DELETE FROM " + dbSchemaConfig.getSchemaName() + ".POSITION_TOKEN_REGISTRY WHERE FIRST_DATE_EVENT < ?";
        int deleted = jdbcTemplate.update(sql, cutoff);
        log.info("[runId={}][entityName=TOKEN_REGISTRY_PURGE][phase=END] cutoff={} deleted={}", runId, cutoff, deleted);
        return deleted;
    }
}
