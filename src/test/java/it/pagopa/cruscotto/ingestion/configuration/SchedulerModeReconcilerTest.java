package it.pagopa.cruscotto.ingestion.configuration;

import org.junit.jupiter.api.Test;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    /**
     * La lista di potatura deve coprire <strong>ogni</strong> job di ingestion registrato, e gli altri
     * test di questa classe non possono accorgersene: iterano sulla lista stessa, quindi un job nuovo
     * dimenticato li lascia tutti verdi.
     *
     * <p>E' gia' successo: {@code statisticsRefreshJob} e' stato aggiunto a {@code QuartzConfiguration}
     * senza finire qui. Il job store e' persistente, quindi in modalita' MASSIVE_SEARCH il suo trigger
     * sarebbe sopravvissuto al cambio di modalita' e avrebbe continuato a far partire un ANALYZE su
     * tutti i padri partizionati — proprio mentre l'ingestor si crede fermo.</p>
     *
     * <p>L'insieme atteso e' dedotto per riflessione dai metodi {@code *JobDetail()} di
     * {@code QuartzConfiguration}, cosi' il test segue l'aggiunta di job futuri senza manutenzione.</p>
     */
    @Test
    void thePruneListMustCoverEveryRegisteredIngestionJob() {
        Set<String> registered = Arrays.stream(QuartzConfiguration.class.getDeclaredMethods())
            .filter(m -> JobDetail.class.equals(m.getReturnType()))
            .filter(m -> m.getParameterCount() == 0)
            // Convenzione verificata su tutti i JobDetail: withIdentity("xJob") <- metodo xJobDetail().
            .map(m -> m.getName().replaceAll("Detail$", ""))
            .collect(Collectors.toSet());

        assertFalse(registered.isEmpty(), "nessun JobDetail trovato: convenzione di naming cambiata?");

        Set<String> covered = new HashSet<>(SchedulerModeReconciler.INGESTION_JOB_NAMES);
        covered.add(SchedulerModeReconciler.MASSIVE_SEARCH_SCANNER_JOB);

        registered.removeAll(covered);
        assertTrue(registered.isEmpty(),
            "job registrati ma mai potati al cambio di modalita': " + registered);
    }
}