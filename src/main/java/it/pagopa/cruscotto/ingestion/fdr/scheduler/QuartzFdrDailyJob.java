package it.pagopa.cruscotto.ingestion.fdr.scheduler;

import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrDailyService;
import it.pagopa.cruscotto.ingestion.scheduler.TrackedJobExecutor;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Il trigger dell'elaborazione giornaliera FDR.
 *
 * <p>Sottile per scelta: decide solo <em>quando</em>, mentre il <em>cosa</em> sta in
 * {@link FdrDailyService}. Cosi' la logica resta verificabile senza far girare uno scheduler.
 *
 * <p>Quartz e' clusterizzato, quindi un trigger scatta su un solo pod; {@code @DisallowConcurrentExecution}
 * copre il caso in cui una giornata duri piu' dell'intervallo fra due scatti, che in DEV — con un
 * cron ogni due minuti — e' la norma e non l'eccezione.
 */
@Component
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
@DisallowConcurrentExecution
public class QuartzFdrDailyJob extends QuartzJobBean {

    @Autowired
    private FdrDailyService dailyService;

    @Autowired
    private TrackedJobExecutor trackedJobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        // Il prefisso FDR_ nell'entity name non e' decorativo: e' cosi' che il rilevatore di
        // giornate abbandonate riconosce, in INGEST_EXECUTION_LOG, un'esecuzione FDR ancora viva.
        trackedJobExecutor.runTracked("FDR_DAILY", "quartz-FDR_DAILY", runId,
                () -> dailyService.runDaily());
    }
}
