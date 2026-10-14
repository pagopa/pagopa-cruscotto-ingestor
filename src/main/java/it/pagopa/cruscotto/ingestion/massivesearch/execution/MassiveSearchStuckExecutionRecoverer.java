package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import lombok.extern.slf4j.Slf4j;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
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

        try (MdcScope ignored = MdcScope.phase("STUCK_EXECUTION_RECOVERY_START")
                     .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH");
             LogDetails details = LogDetails.open()
                     .with("timeout_minutes", timeoutMinutes)
                     .with("threshold", threshold)) {
            log.debug("Started stuck-execution recovery");
        }

        List<StuckExecutionRef> stuck = executionRepository.findStuckRunningExecutions(threshold);
        int recovered = 0;
        for (StuckExecutionRef ref : stuck) {
            try (MdcScope ignored = MdcScope.open().with(LogFields.CTX_INSTANCE_ID, String.valueOf(ref.instanceId())).with(LogFields.CTX_EXECUTION_ID, String.valueOf(ref.executionId()))) {
                boolean executionFailed = executionRepository.recoverStuck(ref.executionId());
                if (!executionFailed) {
                    // The execution completed (or was already failed) between the scan and the update.
                    try (MdcScope phase = MdcScope.phase("STUCK_EXECUTION_RECOVERY_SKIPPED");
                         LogDetails details = LogDetails.open()
                                 .with(LogFields.REASON, "no-longer-running")) {
                        log.info("Skipped recovery, the execution is no longer running");
                    }
                    continue;
                }
                instanceRepository.markFailedFromRunning(ref.instanceId());
                recovered++;
                try (MdcScope phase = MdcScope.phase("STUCK_EXECUTION_RECOVERED")
                             .with(LogFields.EVENT_OUTCOME, "failure");
                     LogDetails details = LogDetails.open().with("error_code", STUCK_ERROR_CODE)) {
                    log.warn("Recovered a stuck execution, marked FAILED and instance released");
                }
            } catch (RuntimeException e) {
                try (MdcScope phase = MdcScope.phase("STUCK_EXECUTION_RECOVERY_ERROR")
                             .with(LogFields.EVENT_OUTCOME, "failure");
                     LogDetails details = LogDetails.open().with(LogFields.ERROR_DETAIL, e.getMessage())) {
                    log.error("Failed to recover a stuck execution", e);
                }
            }
        }

        try (MdcScope ignored = MdcScope.phase("STUCK_EXECUTION_RECOVERY_COMPLETED")
                     .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH");
             LogDetails details = LogDetails.open()
                     .with("candidates", stuck.size())
                     .with("recovered", recovered)) {
            log.debug("Completed stuck-execution recovery");
        }
        return recovered;
    }
}
