package it.pagopa.cruscotto.ingestion.fdr.scheduler;

import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrCommandService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

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
    private FdrQuartzJobExecutor jobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        jobExecutor.run("FDR_COMMAND", () -> commandService.pollAndRun());
    }
}
