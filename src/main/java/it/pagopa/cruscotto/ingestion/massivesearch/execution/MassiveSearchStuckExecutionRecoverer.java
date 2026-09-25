package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Recovers Massive Search executions stuck in {@code RUNNING} (e.g. because the JVM/pod died during
 * processing). Without this, with per-instance concurrency disabled, the instance would stay
 * {@code RUNNING} forever and never be executed again.
 *
 * <p>Invoked by the scanner before it looks for new READY instances. Every transition is a
 * conditional DB update, so a genuinely completing execution is never clobbered.</p>
 */
@Slf4j
@Service
public class MassiveSearchStuckExecutionRecoverer {

    private static final String STUCK_ERROR_CODE = "STUCK_EXECUTION";

    private final MassiveSearchProperties properties;
    private final SearchExecutionRepository executionRepository;
    private final SearchInstanceRepository instanceRepository;

    public MassiveSearchStuckExecutionRecoverer(
        MassiveSearchProperties properties,
        SearchExecutionRepository executionRepository,
        SearchInstanceRepository instanceRepository
    ) {
        this.properties = properties;
        this.executionRepository = executionRepository;
        this.instanceRepository = instanceRepository;
    }

    /**
     * Fails every stuck execution and its owning instance.
     *
     * @return the number of executions actually recovered
     */
    public int recoverStuckExecutions() {
        int timeoutMinutes = Math.max(1, properties.getExecution().getRunningTimeoutMinutes());
        OffsetDateTime threshold = OffsetDateTime.now().minusMinutes(timeoutMinutes);

        log.info("phase=STUCK_EXECUTION_RECOVERY_START entityName=MASSIVE_SEARCH timeoutMinutes={} threshold={}",
            timeoutMinutes, threshold);

        List<StuckExecutionRef> stuck = executionRepository.findStuckRunningExecutions(threshold);
        int recovered = 0;
        for (StuckExecutionRef ref : stuck) {
            MDC.put("instanceId", String.valueOf(ref.instanceId()));
            MDC.put("executionId", String.valueOf(ref.executionId()));
            try {
                boolean executionFailed = executionRepository.recoverStuck(ref.executionId());
                if (!executionFailed) {
                    // The execution completed (or was already failed) between the scan and the update.
                    log.info("phase=STUCK_EXECUTION_RECOVERY_SKIPPED entityName=MASSIVE_SEARCH instanceId={} executionId={} reason=no-longer-running",
                        ref.instanceId(), ref.executionId());
                    continue;
                }
                instanceRepository.markFailedFromRunning(ref.instanceId());
                recovered++;
                log.warn("phase=STUCK_EXECUTION_RECOVERED entityName=MASSIVE_SEARCH instanceId={} executionId={} status=FAILED errorCode={}",
                    ref.instanceId(), ref.executionId(), STUCK_ERROR_CODE);
            } catch (RuntimeException e) {
                log.error("phase=STUCK_EXECUTION_RECOVERY_ERROR entityName=MASSIVE_SEARCH instanceId={} executionId={} reason={}",
                    ref.instanceId(), ref.executionId(), e.getMessage(), e);
            } finally {
                MDC.remove("executionId");
                MDC.remove("instanceId");
            }
        }

        log.info("phase=STUCK_EXECUTION_RECOVERY_COMPLETED entityName=MASSIVE_SEARCH candidates={} recovered={}",
            stuck.size(), recovered);
        return recovered;
    }
}
