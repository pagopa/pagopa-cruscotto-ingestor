package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.JobParameterKeys;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import lombok.extern.slf4j.Slf4j;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@DisallowConcurrentExecution
public class QuartzPositionTransfersImportJob extends QuartzJobBean {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job positionTransfersImportJob;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = EntityName.POSITION_TRANSFERS.name();
        log.info("START runId={} entityName={}", runId, entityName);
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString(JobParameterKeys.RUN_ID, runId)
                    .addLong(JobParameterKeys.SCHEDULED_FIRE_TIME, context.getScheduledFireTime().getTime())
                    .addString(JobParameterKeys.ENTITY_NAME, entityName)
                    .addLong(JobParameterKeys.TIME, System.currentTimeMillis())
                    .toJobParameters();
            jobLauncher.run(positionTransfersImportJob, jobParameters);
        } catch (Exception e) {
            log.error("ERROR runId={} entityName={}", runId, entityName, e);
            throw new JobExecutionException(e);
        } finally {
            log.info("END runId={} entityName={}", runId, entityName);
        }
    }
}
