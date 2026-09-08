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

import java.util.Date;
import java.util.UUID;

@Slf4j
@Component
@DisallowConcurrentExecution
public class QuartzPositionImportJob extends QuartzJobBean {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job positionImportJob;

    @Autowired
    private TrackedJobExecutor trackedJobExecutor;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = EntityName.POSITION.name();
        Date nextFireTime = context.getNextFireTime();
        log.info("jobTag=positionJob START runId={} entityName={} scheduledFireTime={} nextFireTime={}",
                runId, entityName, context.getScheduledFireTime(), nextFireTime);
        try {
            // runFailSafe: ritenta i fallimenti transitori di serializzazione/lock al lancio (collisioni
            // sui metadati Spring Batch sotto SERIALIZABLE) e registra in INGEST_EXECUTION_LOG solo il
            // fallimento finale — una failure prima che il runner crei la sua riga resterebbe altrimenti
            // solo nel log applicativo.
            trackedJobExecutor.runFailSafe(entityName, "batch-" + entityName, runId, () -> {
                JobParameters jobParameters = new JobParametersBuilder()
                        .addString(JobParameterKeys.RUN_ID, runId)
                        .addLong(JobParameterKeys.SCHEDULED_FIRE_TIME, context.getScheduledFireTime().getTime())
                        .addString(JobParameterKeys.ENTITY_NAME, entityName)
                        .addLong(JobParameterKeys.TIME, System.currentTimeMillis())
                        .toJobParameters();
                jobLauncher.run(positionImportJob, jobParameters);
            });
        } finally {
            log.info("jobTag=positionJob END runId={} entityName={}", runId, entityName);
        }
    }
}
