package it.pagopa.cruscotto.ingestion.runmode;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import it.pagopa.cruscotto.ingestion.configuration.AppModeProperties;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.stereotype.Component;

/**
 * Prometheus gauges for the run mode: {@code ingestor.scheduler.up} (1 when the scheduler is running)
 * and {@code ingestor.scheduler.executing.jobs} (jobs executing on this node), both tagged with the
 * active mode. Lets a dashboard/alert tell whether the ingestor is working without reading the DB log.
 */
@Component
public class RunModeMetrics {

    private final Scheduler scheduler;

    public RunModeMetrics(MeterRegistry registry, AppModeProperties appModeProperties, Scheduler scheduler) {
        this.scheduler = scheduler;
        Tags tags = Tags.of("mode", appModeProperties.getMode().name());
        registry.gauge("ingestor.scheduler.up", tags, this, RunModeMetrics::schedulerUp);
        registry.gauge("ingestor.scheduler.executing.jobs", tags, this, RunModeMetrics::executingJobs);
    }

    private double schedulerUp() {
        try {
            return scheduler.isStarted() && !scheduler.isInStandbyMode() && !scheduler.isShutdown() ? 1d : 0d;
        } catch (SchedulerException e) {
            return 0d;
        }
    }

    private double executingJobs() {
        try {
            return scheduler.getCurrentlyExecutingJobs().size();
        } catch (SchedulerException e) {
            return -1d;
        }
    }
}