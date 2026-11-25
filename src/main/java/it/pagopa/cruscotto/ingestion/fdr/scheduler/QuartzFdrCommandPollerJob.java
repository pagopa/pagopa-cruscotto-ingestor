package it.pagopa.cruscotto.ingestion.fdr.scheduler;

import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrCommandService;
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
 * Raccoglie le richieste amministrative lasciate a database dal backend.
 *
 * <p>E' il sostituto di un endpoint REST, ed e' una scelta: un endpoint richiederebbe che
 * l'Ingestor sia raggiungibile e su nel momento esatto in cui qualcuno preme il pulsante. Una riga
 * a database aspetta.
 */
@Component
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
@DisallowConcurrentExecution
public class QuartzFdrCommandPollerJob extends QuartzJobBean {

    @Autowired
    private FdrCommandService commandService;

    @Autowired
    private TrackedJobExecutor trackedJobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        trackedJobExecutor.runTracked("FDR_COMMAND", "quartz-FDR_COMMAND", runId,
                () -> commandService.pollAndRun());
    }
}
