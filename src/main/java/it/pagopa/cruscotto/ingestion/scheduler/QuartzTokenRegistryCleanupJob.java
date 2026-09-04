package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.service.TokenRegistryCleanupService;
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
public class QuartzTokenRegistryCleanupJob extends QuartzJobBean {

    @Autowired
    private TokenRegistryCleanupService tokenRegistryCleanupService;

    @Autowired
    private TrackedJobExecutor trackedJobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = "TOKEN_REGISTRY_PURGE";

        log.info("START runId={} entityName={} phase=START", runId, entityName);
        try {
            // Cleanup jobs used to leave no trace in INGEST_EXECUTION_LOG: a failure was visible only
            // in the application log. The wrapper owns the lifecycle so both runs and errors land there.
            trackedJobExecutor.runTracked(entityName, "quartz-" + entityName, runId,
                    () -> tokenRegistryCleanupService.cleanup(runId));
        } finally {
            log.info("END runId={} entityName={} phase=END", runId, entityName);
        }
    }
}
