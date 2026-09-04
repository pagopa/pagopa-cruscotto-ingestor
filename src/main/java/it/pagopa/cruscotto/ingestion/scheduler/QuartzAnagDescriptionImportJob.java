package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.AnagDescriptionIngestionRunner;
import it.pagopa.cruscotto.ingestion.batch.JobParameterKeys;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.UUID;

@Slf4j
@Component
@DisallowConcurrentExecution
public class QuartzAnagDescriptionImportJob extends QuartzJobBean {
    @Autowired
    private AnagDescriptionIngestionRunner anagDescriptionIngestionRunner;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = EntityName.ANAG_DESCRIPTION_REFRESH.name();
        Date nextFireTime = context.getNextFireTime();
        log.info("jobTag=anagDescriptionJob START runId={} entityName={} scheduledFireTime={} nextFireTime={}",
                runId, entityName, context.getScheduledFireTime(), nextFireTime);
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString(JobParameterKeys.RUN_ID, runId)
                    .addString(JobParameterKeys.ENTITY_NAME, entityName)
                    .addLong(JobParameterKeys.SCHEDULED_FIRE_TIME, context.getScheduledFireTime().getTime())
                    .addLong(JobParameterKeys.TIME, System.currentTimeMillis())
                    .toJobParameters();
            // Direct call (not via jobLauncher): the runner owns the execution-log row and already
            // writes STARTED/COMPLETED/FAILED with the real counters and root error. A recordFailure
            // here would run a second logFailed that overwrites that row with zero counters and a
            // generic code, so it is intentionally NOT called (see other jobs that use jobLauncher,
            // where the launcher swallows step failures and the safety net is warranted).
            anagDescriptionIngestionRunner.run(jobParameters);
        } catch (Throwable t) {
            log.error("jobTag=anagDescriptionJob ERROR runId={} entityName={} error={}", runId, entityName, t.getMessage(), t);
            throw new JobExecutionException(t);
        } finally {
            log.info("jobTag=anagDescriptionJob END runId={} entityName={}", runId, entityName);
        }
    }
}
