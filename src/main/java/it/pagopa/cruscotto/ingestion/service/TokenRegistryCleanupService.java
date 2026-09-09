package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.ZoneOffset;

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

        // retention is a Duration; LocalDate.minus(Duration) throws UnsupportedTemporalTypeException
        // ("Unsupported unit: Seconds") because a LocalDate has no time component. Subtract whole days.
        LocalDate cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(config.getRetention().toDays());
        String sql = "DELETE FROM " + dbSchemaConfig.getSchemaName() + ".POSITION_TOKEN_REGISTRY WHERE FIRST_DATE_EVENT < ?";
        int deleted = jdbcTemplate.update(sql, cutoff);
        log.info("[runId={}][entityName=TOKEN_REGISTRY_PURGE][phase=END] cutoff={} deleted={}", runId, cutoff, deleted);
        return deleted;
    }
}
