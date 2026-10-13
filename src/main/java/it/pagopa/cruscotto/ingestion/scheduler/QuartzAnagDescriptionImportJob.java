package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.AnagDescriptionIngestionRunner;
import it.pagopa.cruscotto.ingestion.batch.JobParameterKeys;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

@Component
@DisallowConcurrentExecution
public class QuartzAnagDescriptionImportJob extends QuartzJobBean {
    @Autowired
    private AnagDescriptionIngestionRunner anagDescriptionIngestionRunner;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = EntityName.ANAG_DESCRIPTION_REFRESH.name();
        RunContext ctx = new RunContext(entityName, runId, Instant.now());
        Date nextFireTime = context.getNextFireTime();

        // Questo job non passa da TrackedJobExecutor, che e' il punto in cui tutti gli altri
        // aprono lo scope MDC. Senza questo blocco ctx_instance_id mancherebbe su tutte le sue
        // righe, comprese quelle delle query ADX che scatena: sui tre pod non si saprebbe quale
        // istanza ha fatto cosa. correlation_id ed event_action li riscrive LogHelper a ogni
        // chiamata, quindi restano garantiti anche fuori da qui.
        try (MdcScope ignored = MdcScope.open()
                .with(LogFields.CTX_INSTANCE_ID, ExecutionLogService.getInstanceId())) {
            run(context, ctx, nextFireTime);
        }
    }

    private void run(JobExecutionContext context, RunContext ctx, Date nextFireTime)
            throws JobExecutionException {
        try (LogDetails ignored = LogDetails.open().with("next_fire_time", nextFireTime)) {
            LogHelper.info(ctx, RunPhase.START, "Started job");
        }
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString(JobParameterKeys.RUN_ID, ctx.getRunId())
                    .addString(JobParameterKeys.ENTITY_NAME, ctx.getEntityName())
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
            LogHelper.error(ctx, RunPhase.ERROR, "Job failed", t);
            throw new JobExecutionException(t);
        } finally {
            // Nessun event_outcome su END: il finally gira anche dopo un fallimento, e dichiarare
            // success qui renderebbe il campo inservibile per cercare i job andati male.
            LogHelper.info(ctx, RunPhase.END, "Finished job");
        }
    }
}
