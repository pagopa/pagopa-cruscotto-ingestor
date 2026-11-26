package it.pagopa.cruscotto.ingestion.fdr.scheduler;

import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrDailyService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

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
    private FdrQuartzJobExecutor jobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        // Il nome finisce in event_action ed e' cio' su cui si filtrano i log. Non ha piu' alcun
        // ruolo nel rilevare le giornate abbandonate: quello lo fa il lease su FDR_INGESTION_STATUS,
        // da quando FDR ha smesso di dedurre la liveness dal prefisso FDR_ in INGEST_EXECUTION_LOG.
        jobExecutor.run("FDR_DAILY", () -> dailyService.runDaily());
    }
}
