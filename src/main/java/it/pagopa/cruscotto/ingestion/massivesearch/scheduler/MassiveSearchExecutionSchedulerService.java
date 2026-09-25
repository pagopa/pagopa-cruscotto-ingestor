package it.pagopa.cruscotto.ingestion.massivesearch.scheduler;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchStuckExecutionRecoverer;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.SearchInstanceRepository;
import it.pagopa.cruscotto.ingestion.massivesearch.facade.MassiveSearchFacade;
import it.pagopa.cruscotto.ingestion.massivesearch.facade.SearchExecutionStartResult;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
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

        log.info("phase=SCAN_FOUND runId={} entityName=MASSIVE_SEARCH_SCANNER readyInstances={} limit={}",
            runId, instances.size(), limit);

        int submitted = 0;
        for (UUID instanceId : instances) {
            boolean accepted = taskExecutor.submit(() -> runInstance(runId, instanceId));
            if (accepted) {
                submitted++;
                log.info("phase=SCAN_SUBMITTED runId={} entityName=MASSIVE_SEARCH_SCANNER instanceId={}",
                    runId, instanceId);
            } else {
                // Executor saturated: leave the instance READY, it will be retried on the next scan.
                log.warn("phase=SCAN_SKIPPED runId={} entityName=MASSIVE_SEARCH_SCANNER instanceId={} reason=executor-saturated",
                    runId, instanceId);
            }
        }

        log.info("phase=SCAN_DONE runId={} entityName=MASSIVE_SEARCH_SCANNER submitted={} skipped={}",
            runId, submitted, instances.size() - submitted);
        return submitted;
    }

    private void runInstance(String runId, UUID instanceId) {
        MDC.put("entityName", "MASSIVE_SEARCH");
        MDC.put("instanceId", String.valueOf(instanceId));
        try {
            // The facade atomically acquires the per-instance RUNNING lock, so an instance already
            // running (or picked up by another scan) is rejected here without side effects.
            SearchExecutionStartResult result = facade.execute(instanceId);
            log.info("phase=SCAN_EXECUTION_DISPATCHED runId={} entityName=MASSIVE_SEARCH instanceId={} executionId={} status={}",
                runId, instanceId, result.executionId(), result.status());
        } catch (RuntimeException e) {
            log.error("phase=SCAN_EXECUTION_ERROR runId={} entityName=MASSIVE_SEARCH instanceId={} reason={}",
                runId, instanceId, e.getMessage(), e);
        } finally {
            MDC.remove("instanceId");
            MDC.remove("entityName");
        }
    }
}
