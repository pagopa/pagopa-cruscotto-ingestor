package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.service.ExecutionLogCleanupService;
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
public class QuartzExecutionLogCleanupJob extends QuartzJobBean {

    @Autowired
    private ExecutionLogCleanupService executionLogCleanupService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = "EXECUTION_LOG_CLEANUP";

        log.info("START runId={} entityName={} phase=START", runId, entityName);
        try {
            executionLogCleanupService.cleanup(runId);
        } catch (Throwable t) {
            log.error("ERROR runId={} entityName={} phase=ERROR message={}", runId, entityName, t.getMessage(), t);
            throw new JobExecutionException(t);
        } finally {
            log.info("END runId={} entityName={} phase=END", runId, entityName);
        }
    }
}
