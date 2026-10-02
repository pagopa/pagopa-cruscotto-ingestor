package it.pagopa.cruscotto.ingestion.configuration;

import lombok.extern.slf4j.Slf4j;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Reconciles the persistent (clustered) Quartz job store with the active {@link AppRunMode} at startup.
 *
 * <p>The JDBC job store is shared and durable: triggers scheduled by a previous run in a different mode
 * survive a redeploy and would keep firing regardless of what the current boot registers. Without this
 * reconciler a mode switch (e.g. via the toggle pipeline) would NOT be effective — e.g. switching from
 * BOTH to MASSIVE_SEARCH would leave the ingestion triggers in the store still firing. This removes the
 * jobs that must not run in the current mode:</p>
 * <ul>
 *   <li>MASSIVE_SEARCH-only: removes the ADX ingestion jobs;</li>
 *   <li>INGESTOR-only: removes the Massive Search scanner;</li>
 *   <li>BOTH: removes nothing.</li>
 * </ul>
 *
 * <p>Runs before the scheduler is started (as a {@link SmartInitializingSingleton}), so stale triggers
 * are pruned before they can fire. {@link Scheduler#deleteJob} also removes the job's triggers and is a
 * no-op when the job is absent, so this is idempotent and safe to run on every node of the cluster. When
 * the scheduler is disabled entirely ({@code app.scheduler-enabled=false}) the store is left untouched.</p>
 */
@Slf4j
@Component
public class SchedulerModeReconciler implements SmartInitializingSingleton {

    /** Must match the JobDetail identities registered in {@link QuartzConfiguration}. */
    static final List<String> INGESTION_JOB_NAMES = List.of(
        "positionImportJob", "positionTokensImportJob", "positionTransfersImportJob",
        "extraInfoImportJob", "eventsWfImportJob", "anagDescriptionImportJob", "reconciliationJob",
        "tokenRegistryCleanupJob", "stagingErrorCleanupJob", "batchMetadataCleanupJob",
        "executionLogCleanupJob");

    /** Must match the scanner JobDetail identity in {@code MassiveSearchSchedulerConfiguration}. */
    static final String MASSIVE_SEARCH_SCANNER_JOB = "massiveSearchExecutionScannerJob";

    private final Scheduler scheduler;
    private final AppModeProperties appModeProperties;

    public SchedulerModeReconciler(Scheduler scheduler, AppModeProperties appModeProperties) {
        this.scheduler = scheduler;
        this.appModeProperties = appModeProperties;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (!appModeProperties.schedulerShouldStart()) {
            // Scheduler fully disabled: nothing runs anyway, leave the store as-is for when re-enabled.
            return;
        }
        if (!appModeProperties.ingestionActive()) {
            prune(INGESTION_JOB_NAMES, "ingestion");
        }
        if (!appModeProperties.massiveSearchActive()) {
            prune(List.of(MASSIVE_SEARCH_SCANNER_JOB), "massive-search-scanner");
        }
    }

    private void prune(List<String> jobNames, String label) {
        int removed = 0;
        for (String name : jobNames) {
            try {
                if (scheduler.deleteJob(JobKey.jobKey(name))) {
                    removed++;
                    log.info("phase=SCHEDULER_MODE_PRUNE mode={} group={} job={} removed=true",
                        appModeProperties.getMode(), label, name);
                }
            } catch (SchedulerException e) {
                log.warn("phase=SCHEDULER_MODE_PRUNE_FAILED mode={} group={} job={} reason={}",
                    appModeProperties.getMode(), label, name, e.getMessage());
            }
        }
        log.info("phase=SCHEDULER_MODE_RECONCILED mode={} group={} removedJobs={}",
            appModeProperties.getMode(), label, removed);
    }
}