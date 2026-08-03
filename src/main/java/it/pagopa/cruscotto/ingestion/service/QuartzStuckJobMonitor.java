package it.pagopa.cruscotto.ingestion.service;

import io.micrometer.core.instrument.MeterRegistry;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Watchdog that detects Quartz jobs executing beyond {@code ingestion.health.blocked-trigger-threshold}.
 * A run that hangs (e.g. on a DB write/lock or a dead connection) keeps its job executing forever which,
 * with {@code @DisallowConcurrentExecution}, leaves the trigger BLOCKED and freezes the whole downstream
 * chain. When a stuck job is found this emits a structured ERROR log (searchable on Elastic via
 * {@code phase=STUCK_JOB}), publishes the {@code ingestor.quartz.stuck.jobs} Prometheus gauge and exposes
 * the current stuck list to {@link QuartzBlockedTriggersHealthIndicator}.
 * <p>
 * Note: {@link Scheduler#getCurrentlyExecutingJobs()} reports jobs running on this node only; in a
 * multi-node cluster a job stuck on another node would not be observed here.
 */
@Slf4j
@Component
public class QuartzStuckJobMonitor {

    private final Scheduler scheduler;
    private final IngestionConfig ingestionConfig;

    private volatile List<StuckJob> stuckJobs = List.of();

    public QuartzStuckJobMonitor(Scheduler scheduler, IngestionConfig ingestionConfig, MeterRegistry meterRegistry) {
        this.scheduler = scheduler;
        this.ingestionConfig = ingestionConfig;
        meterRegistry.gauge("ingestor.quartz.stuck.jobs", this, monitor -> monitor.stuckJobs.size());
    }

    @Scheduled(
            fixedDelayString = "${ingestion.health.check-interval-ms:60000}",
            initialDelayString = "${ingestion.health.check-interval-ms:60000}")
    public void scan() {
        IngestionConfig.HealthConfig cfg = ingestionConfig.getHealth();
        if (cfg == null || !cfg.isEnabled()) {
            stuckJobs = List.of();
            return;
        }

        Duration threshold = cfg.getBlockedTriggerThreshold();
        long thresholdMs = threshold != null ? threshold.toMillis() : 0L;
        if (thresholdMs <= 0) {
            stuckJobs = List.of();
            return;
        }

        try {
            List<JobExecutionContext> running = scheduler.getCurrentlyExecutingJobs();
            long now = System.currentTimeMillis();
            List<StuckJob> detected = new ArrayList<>();

            for (JobExecutionContext jec : running) {
                Date fireTime = jec.getFireTime();
                if (fireTime == null) {
                    continue;
                }
                long runtimeMs = now - fireTime.getTime();
                if (runtimeMs <= thresholdMs) {
                    continue;
                }

                String jobName = jec.getJobDetail().getKey().getName();
                String triggerName = jec.getTrigger().getKey().getName();
                Instant firedAt = fireTime.toInstant();
                detected.add(new StuckJob(jobName, triggerName, firedAt, runtimeMs));

                log.error("[phase=STUCK_JOB][jobTag={}][job={}][trigger={}][fireTime={}][runtimeMs={}][thresholdMs={}] " +
                                "Quartz job running beyond threshold; likely hung on a DB write/lock or dead connection " +
                                "and blocking its trigger (@DisallowConcurrentExecution). Correlate with the run's runId " +
                                "in the job logs, capture a thread dump, then restart to release the trigger.",
                        jobName, jobName, triggerName, firedAt, runtimeMs, thresholdMs);
            }

            stuckJobs = List.copyOf(detected);
        } catch (SchedulerException ex) {
            log.warn("[phase=STUCK_JOB] Unable to inspect currently executing Quartz jobs: {}", ex.getMessage());
        }
    }

    public List<StuckJob> getStuckJobs() {
        return stuckJobs;
    }

    public record StuckJob(String jobName, String triggerName, Instant fireTime, long runtimeMs) {
    }
}
