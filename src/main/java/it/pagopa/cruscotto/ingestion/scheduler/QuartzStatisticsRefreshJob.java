package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.service.StatisticsRefreshService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@DisallowConcurrentExecution
public class QuartzStatisticsRefreshJob extends QuartzJobBean {

    @Autowired
    private StatisticsRefreshService statisticsRefreshService;

    @Autowired
    private TrackedJobExecutor trackedJobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = "STATISTICS_REFRESH";

        // Tracciato come gli altri job di manutenzione: un ANALYZE che fallisce ripetutamente va
        // visto in INGEST_EXECUTION_LOG, non solo nei log applicativi.
        trackedJobExecutor.runTracked(entityName, "quartz-" + entityName, runId,
                () -> statisticsRefreshService.refresh(runId));
    }
}
