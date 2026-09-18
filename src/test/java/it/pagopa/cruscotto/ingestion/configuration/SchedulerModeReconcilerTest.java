package it.pagopa.cruscotto.ingestion.configuration;

import org.junit.jupiter.api.Test;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the startup reconciliation that prunes the persistent Quartz job store to match the run
 * mode: ingestion jobs removed in MASSIVE_SEARCH, the scanner removed in INGESTOR, nothing in BOTH,
 * and the store left untouched when the scheduler is disabled.
 */
class SchedulerModeReconcilerTest {

    private final Scheduler scheduler = mock(Scheduler.class);

    private SchedulerModeReconciler reconciler(AppRunMode mode, boolean enabled) throws SchedulerException {
        when(scheduler.deleteJob(any())).thenReturn(true);
        AppModeProperties props = new AppModeProperties();
        props.setMode(mode);
        props.setSchedulerEnabled(enabled);
        return new SchedulerModeReconciler(scheduler, props);
    }

    @Test
    void massiveSearchModeRemovesIngestionJobsButKeepsScanner() throws Exception {
        reconciler(AppRunMode.MASSIVE_SEARCH, true).afterSingletonsInstantiated();

        for (String name : SchedulerModeReconciler.INGESTION_JOB_NAMES) {
            verify(scheduler).deleteJob(JobKey.jobKey(name));
        }
        verify(scheduler, never()).deleteJob(JobKey.jobKey(SchedulerModeReconciler.MASSIVE_SEARCH_SCANNER_JOB));
    }

    @Test
    void ingestorModeRemovesScannerButKeepsIngestionJobs() throws Exception {
        reconciler(AppRunMode.INGESTOR, true).afterSingletonsInstantiated();

        verify(scheduler).deleteJob(JobKey.jobKey(SchedulerModeReconciler.MASSIVE_SEARCH_SCANNER_JOB));
        for (String name : SchedulerModeReconciler.INGESTION_JOB_NAMES) {
            verify(scheduler, never()).deleteJob(JobKey.jobKey(name));
        }
    }

    @Test
    void bothModePrunesNothing() throws Exception {
        reconciler(AppRunMode.BOTH, true).afterSingletonsInstantiated();

        verify(scheduler, never()).deleteJob(any());
    }

    @Test
    void disabledSchedulerLeavesStoreUntouched() throws Exception {
        // schedulerShouldStart() is false regardless of mode when the master switch is off.
        reconciler(AppRunMode.MASSIVE_SEARCH, false).afterSingletonsInstantiated();

        verify(scheduler, never()).deleteJob(any());
    }
}