package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.JobParameterKeys;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.CheckpointStoreService;
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

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Component
@DisallowConcurrentExecution
public class QuartzEventsWfImportJob extends QuartzJobBean {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job eventsWfImportJob;

    @Autowired
    private IngestionConfig ingestionConfig;

    @Autowired
    private CheckpointStoreService checkpointStoreService;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String runId = UUID.randomUUID().toString();
        String entityName = EntityName.EVENTS_WF.name();
        Date nextFireTime = context.getNextFireTime();
        log.info("jobTag=eventsWfJob START runId={} entityName={} scheduledFireTime={} nextFireTime={}",
                runId, entityName, context.getScheduledFireTime(), nextFireTime);
        try {
            runBatchJob(context, runId, entityName);
            executeDedicatedBurstRuns(context, entityName);
        } catch (Throwable t) {
            log.error("jobTag=eventsWfJob ERROR runId={} entityName={} error={}", runId, entityName, t.getMessage(), t);
            throw new JobExecutionException(t);
        } finally {
            log.info("jobTag=eventsWfJob END runId={} entityName={}", runId, entityName);
        }
    }

    private void executeDedicatedBurstRuns(JobExecutionContext context, String entityName) throws Exception {
        IngestionConfig.EventsWfConfig.DedicatedConfig dedicatedConfig = ingestionConfig.getEventsWf().getDedicated();
        if (dedicatedConfig == null || !dedicatedConfig.isEnabled()) {
            return;
        }

        int extraRuns = Math.max(0, dedicatedConfig.getExtraRunsWhenBacklogged());
        Duration backlogThreshold = positiveOrDefault(dedicatedConfig.getBacklogThreshold(), Duration.ofHours(2));
        for (int burstIndex = 1; burstIndex <= extraRuns; burstIndex++) {
            Optional<Duration> backlogLag = resolveBacklogLag();
            if (backlogLag.isEmpty() || backlogLag.orElseThrow(
                    () -> new IllegalStateException("Backlog lag unexpectedly absent")).compareTo(backlogThreshold) < 0) {
                return;
            }

            String burstRunId = UUID.randomUUID().toString();
            Duration lag = backlogLag.orElseThrow(
                    () -> new IllegalStateException("Backlog lag unexpectedly absent for burst"));
            log.info("jobTag=eventsWfJob CATCH_UP_BURST runId={} entityName={} burstIndex={} lag={} threshold={}",
                    burstRunId, entityName, burstIndex, lag, backlogThreshold);
            runBatchJob(context, burstRunId, entityName);
        }
    }

    private Optional<Duration> resolveBacklogLag() {
        Optional<Instant> parentCheckpoint = checkpointStoreService.getCheckpoint(EntityName.POSITION_TOKENS);
        Optional<Instant> eventsWfCheckpoint = checkpointStoreService.getCheckpoint(EntityName.EVENTS_WF);
        if (parentCheckpoint.isEmpty() || eventsWfCheckpoint.isEmpty()) {
            return Optional.empty();
        }

        Instant parentTs = parentCheckpoint.orElseThrow(
                () -> new IllegalStateException("Parent checkpoint unexpectedly absent"));
        Instant eventsTs = eventsWfCheckpoint.orElseThrow(
                () -> new IllegalStateException("EVENTS_WF checkpoint unexpectedly absent"));
        if (!eventsTs.isBefore(parentTs)) {
            return Optional.empty();
        }

        return Optional.of(Duration.between(eventsTs, parentTs));
    }

    private void runBatchJob(JobExecutionContext context, String runId, String entityName) throws Exception {
        jobLauncher.run(eventsWfImportJob, new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, runId)
                .addLong(JobParameterKeys.SCHEDULED_FIRE_TIME, resolveScheduledFireTime(context))
                .addString(JobParameterKeys.ENTITY_NAME, entityName)
                .addLong(JobParameterKeys.TIME, System.currentTimeMillis())
                .toJobParameters());
    }

    private long resolveScheduledFireTime(JobExecutionContext context) {
        Date scheduledFireTime = context.getScheduledFireTime();
        if (scheduledFireTime == null) {
            return System.currentTimeMillis();
        }
        return scheduledFireTime.getTime();
    }

    private Duration positiveOrDefault(Duration candidate, Duration fallback) {
        if (candidate == null || candidate.isNegative() || candidate.isZero()) {
            return fallback;
        }
        return candidate;
    }
}
