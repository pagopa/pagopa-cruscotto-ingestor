package it.pagopa.cruscotto.ingestion.massivesearch.scheduler;

import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchStuckExecutionRecoverer;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.SearchInstanceRepository;
import it.pagopa.cruscotto.ingestion.massivesearch.facade.MassiveSearchFacade;
import it.pagopa.cruscotto.ingestion.massivesearch.facade.SearchExecutionStartResult;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import lombok.extern.slf4j.Slf4j;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * Scans {@code SEARCH_INSTANCE} for {@code READY} instances and hands them over to the Massive Search
 * execution, honouring the per-run and per-concurrency limits.
 *
 * <p>The API Layer only sets {@code status = READY}; this service is the single place that picks the
 * work up (polled by {@link MassiveSearchExecutionQuartzJob}). It contains no report/CSV/business
 * logic: it delegates each instance to {@link MassiveSearchFacade} on the dedicated
 * {@link MassiveSearchTaskExecutor}, so the Quartz thread returns immediately.</p>
 *
 * <p>Each scan first recovers stuck {@code RUNNING} executions, then submits new work.</p>
 */
@Slf4j
@Service
public class MassiveSearchExecutionSchedulerService {

    private final MassiveSearchProperties properties;
    private final SearchInstanceRepository instanceRepository;
    private final MassiveSearchFacade facade;
    private final MassiveSearchTaskExecutor taskExecutor;
    private final MassiveSearchStuckExecutionRecoverer stuckExecutionRecoverer;

    public MassiveSearchExecutionSchedulerService(
        MassiveSearchProperties properties,
        SearchInstanceRepository instanceRepository,
        MassiveSearchFacade facade,
        MassiveSearchTaskExecutor taskExecutor,
        MassiveSearchStuckExecutionRecoverer stuckExecutionRecoverer
    ) {
        this.properties = properties;
        this.instanceRepository = instanceRepository;
        this.facade = facade;
        this.taskExecutor = taskExecutor;
        this.stuckExecutionRecoverer = stuckExecutionRecoverer;
    }

    /**
     * Runs a single scan: recovers stuck executions, then reads up to {@code max-instances-per-run}
     * READY instances and submits each to the dedicated executor. Returns the number of instances
     * actually submitted.
     */
    public int scan(String runId) {
        // 1. Recover executions left RUNNING by a crashed/killed run so their instances are not stuck.
        stuckExecutionRecoverer.recoverStuckExecutions();

        // 2. Find READY instances and submit them.
        int limit = Math.max(1, properties.getScheduler().getMaxInstancesPerRun());
        List<UUID> instances = instanceRepository.findExecutableInstances(limit);

        try (MdcScope ignored = MdcScope.run(runId, "MASSIVE_SEARCH_SCANNER", "SCAN_FOUND");
             LogDetails details = LogDetails.open()
                     .with("ready_instances", instances.size())
                     .with("limit", limit)) {
            log.info("Found instances ready to execute");
        }

        int submitted = 0;
        for (UUID instanceId : instances) {
            boolean accepted = taskExecutor.submit(() -> runInstance(runId, instanceId));
            if (accepted) {
                submitted++;
                try (MdcScope ignored = MdcScope.run(runId, "MASSIVE_SEARCH_SCANNER", "SCAN_SUBMITTED");
                     LogDetails details = LogDetails.open()
                             .with(LogFields.CTX_INSTANCE_ID, String.valueOf(instanceId))) {
                    log.debug("Submitted instance to the execution pool");
                }
            } else {
                // Executor saturated: leave the instance READY, it will be retried on the next scan.
                try (MdcScope ignored = MdcScope.run(runId, "MASSIVE_SEARCH_SCANNER", "SCAN_SKIPPED");
                     LogDetails details = LogDetails.open()
                             .with(LogFields.CTX_INSTANCE_ID, String.valueOf(instanceId))
                             .with(LogFields.REASON, "executor-saturated")) {
                    log.warn("Left instance READY, the execution pool is saturated");
                }
            }
        }

        try (MdcScope ignored = MdcScope.run(runId, "MASSIVE_SEARCH_SCANNER", "SCAN_DONE");
             LogDetails details = LogDetails.open()
                     .with("submitted", submitted)
                     .with("skipped", instances.size() - submitted)) {
            log.info("Completed scan for ready instances");
        }
        return submitted;
    }

    private void runInstance(String runId, UUID instanceId) {
        try (MdcScope ignored = MdcScope.open().with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH").with(LogFields.CTX_INSTANCE_ID, String.valueOf(instanceId))) {
            // The facade atomically acquires the per-instance RUNNING lock, so an instance already
            // running (or picked up by another scan) is rejected here without side effects.
            SearchExecutionStartResult result = facade.execute(instanceId);
            try (MdcScope phase = MdcScope.phase("SCAN_EXECUTION_DISPATCHED");
                 LogDetails details = LogDetails.open()
                         .with(LogFields.CTX_EXECUTION_ID, String.valueOf(result.executionId()))
                         .with("status", String.valueOf(result.status()))) {
                log.info("Dispatched search execution");
            }
        } catch (RuntimeException e) {
            try (MdcScope phase = MdcScope.phase("SCAN_EXECUTION_ERROR")
                         .with(LogFields.EVENT_OUTCOME, "failure");
                 LogDetails details = LogDetails.open().with(LogFields.ERROR_DETAIL, e.getMessage())) {
                log.error("Failed to dispatch a search execution", e);
            }
        }
    }
}
