package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.service.StatisticsRefreshService;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
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

        log.info("START runId={} entityName={} phase=START", runId, entityName);
        try {
            // Tracciato come gli altri job di manutenzione: un ANALYZE che fallisce ripetutamente va
            // visto in INGEST_EXECUTION_LOG, non solo nei log applicativi.
            trackedJobExecutor.runTracked(entityName, "quartz-" + entityName, runId,
                    () -> statisticsRefreshService.refresh(runId));
        } finally {
            log.info("END runId={} entityName={} phase=END", runId, entityName);
        }
    }
}
