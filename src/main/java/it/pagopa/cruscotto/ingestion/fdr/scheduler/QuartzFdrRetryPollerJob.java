package it.pagopa.cruscotto.ingestion.fdr.scheduler;

import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrRetryService;
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
 * Riprende le giornate il cui tentativo successivo e' scaduto.
 *
 * <p>Separato dal poller dei comandi perche' risponde a una domanda diversa — "c'e' qualcosa da
 * riprendere?" invece di "c'e' qualcosa da fare?" — e perche' i due hanno ritmi diversi: i comandi
 * vanno raccolti in fretta, i retry rispettano un'attesa di un'ora scritta a database.
 */
@Component
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
@DisallowConcurrentExecution
public class QuartzFdrRetryPollerJob extends QuartzJobBean {

    @Autowired
    private FdrRetryService retryService;

    @Autowired
    private TrackedJobExecutor trackedJobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        trackedJobExecutor.runTracked("FDR_RETRY", "quartz-FDR_RETRY", runId,
                () -> retryService.pollAndRetry());
    }
}
