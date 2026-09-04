package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.JobParameterKeys;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.UUID;

@Slf4j
@Component
@DisallowConcurrentExecution
public class QuartzReconciliationImportJob extends QuartzJobBean {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("reconciliationJob")
    private Job reconciliationJob;

    @Autowired
    private TrackedJobExecutor trackedJobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = EntityName.RECONCILIATION.name();

        Date nextFireTime = context.getNextFireTime();
        log.info("jobTag=reconciliationJob START runId={} entityName={} scheduledFireTime={} nextFireTime={}",
                runId, entityName, context.getScheduledFireTime(), nextFireTime);
        try {
            // Unlike the ingestion runners, the reconciliation runner writes no execution-log row:
            // the wrapper owns the whole lifecycle (STARTED -> COMPLETED/FAILED) so this job is
            // visible in INGEST_EXECUTION_LOG, errors included.
            trackedJobExecutor.runTracked(entityName, "batch-" + entityName, runId, () -> {
                // SimpleJobLauncher does NOT rethrow when a step fails: it records the failure on the
                // JobExecution and returns. So a swallowed step failure must be surfaced explicitly,
                // otherwise the run would be logged COMPLETED despite having failed.
                JobExecution execution = jobLauncher.run(reconciliationJob, new JobParametersBuilder()
                        .addString(JobParameterKeys.RUN_ID, runId)
                        .addString(JobParameterKeys.ENTITY_NAME, entityName)
                        .addLong(JobParameterKeys.SCHEDULED_FIRE_TIME, context.getScheduledFireTime().getTime())
                        .addLong(JobParameterKeys.TIME, System.currentTimeMillis())
                        .toJobParameters());
                if (execution.getStatus() != BatchStatus.COMPLETED) {
                    throw new IllegalStateException("Reconciliation batch job did not complete: status="
                            + execution.getStatus() + ", exitDescription="
                            + execution.getExitStatus().getExitDescription());
                }
            });
        } finally {
            log.info("jobTag=reconciliationJob END runId={} entityName={}", runId, entityName);
        }
    }
}


