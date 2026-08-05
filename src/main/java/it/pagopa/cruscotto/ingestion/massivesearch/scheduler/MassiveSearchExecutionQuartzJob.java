package it.pagopa.cruscotto.ingestion.massivesearch.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.util.UUID;

/**
 * Quartz scanner that periodically triggers pending Massive Search instances.
 *
 * <p>Pure scanner: it only delegates to {@link MassiveSearchExecutionSchedulerService} and returns
 * quickly. It contains no business logic, generates no report, reads no CSV and never queries
 * POSITION / TOKEN / TRANSFER. {@link DisallowConcurrentExecution} guarantees a single Massive Search
 * scan at a time (matching the ADX ingestion jobs' concurrency policy).</p>
 */
@Slf4j
@DisallowConcurrentExecution
public class MassiveSearchExecutionQuartzJob extends QuartzJobBean {

    private static final String ENTITY_NAME = "MASSIVE_SEARCH_SCANNER";

    @Autowired
    private MassiveSearchExecutionSchedulerService schedulerService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        MDC.put("entityName", ENTITY_NAME);
        MDC.put("runId", runId);
        log.info("jobTag=massiveSearchScanner START runId={} entityName={} scheduledFireTime={} nextFireTime={}",
            runId, ENTITY_NAME, context.getScheduledFireTime(), context.getNextFireTime());
        try {
            int submitted = schedulerService.scan(runId);
            log.info("jobTag=massiveSearchScanner CHECKPOINT runId={} entityName={} submitted={}",
                runId, ENTITY_NAME, submitted);
        } catch (Throwable t) {
            log.error("jobTag=massiveSearchScanner ERROR runId={} entityName={} error={}",
                runId, ENTITY_NAME, t.getMessage(), t);
            throw new JobExecutionException(t);
        } finally {
            log.info("jobTag=massiveSearchScanner END runId={} entityName={}", runId, ENTITY_NAME);
            MDC.remove("runId");
            MDC.remove("entityName");
        }
    }
}
