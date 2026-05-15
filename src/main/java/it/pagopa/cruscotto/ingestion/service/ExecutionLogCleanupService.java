package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.repository.ExecutionLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExecutionLogCleanupService {

    private final ExecutionLogRepository executionLogRepository;

    @Value("${ingestion.executionLog.enabled:true}")
    private boolean enabled;

    @Value("${ingestion.executionLog.retentionDays:2}")
    private int retentionDays;

    @Scheduled(cron = "${ingestion.executionLog.cleanupCron:0 2 * * *}")
    @Transactional
    public void cleanup() {
        if (!enabled) {
            return;
        }
        try {
            OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minusDays(retentionDays);
            long countBefore = executionLogRepository.countByCreatedAtBefore(threshold);

            if (countBefore > 0) {
                long deleted = executionLogRepository.deleteByCreatedAtBefore(threshold);
                log.info("[phase=EXEC_LOG_CLEANUP] Deleted {} execution log records older than {} days", deleted, retentionDays);
            } else {
                log.debug("[phase=EXEC_LOG_CLEANUP] No records to delete");
            }
        } catch (Exception ex) {
            // Best-effort: cleanup failures should not be critical
            log.error("[phase=EXEC_LOG_CLEANUP] Cleanup failed: {}", ex.getMessage(), ex);
        }
    }
}

