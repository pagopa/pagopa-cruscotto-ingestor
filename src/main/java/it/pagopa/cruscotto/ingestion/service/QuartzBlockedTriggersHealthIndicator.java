package it.pagopa.cruscotto.ingestion.service;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Reports {@code DOWN} while one or more Quartz jobs are stuck (running beyond the configured
 * threshold), as detected by {@link QuartzStuckJobMonitor}. Exposed on {@code /management/health}
 * for Prometheus/Alertmanager scraping. It is deliberately not part of the liveness/readiness
 * probe groups, so a stuck job raises an alert without triggering a pod restart.
 * <p>
 * This indicator only reads the monitor's cached state (no scheduler calls, no logging), so it is
 * cheap and side-effect free on every health scrape.
 */
@Component("quartzStuckJobs")
public class QuartzBlockedTriggersHealthIndicator implements HealthIndicator {

    private final QuartzStuckJobMonitor monitor;

    public QuartzBlockedTriggersHealthIndicator(QuartzStuckJobMonitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public Health health() {
        List<QuartzStuckJobMonitor.StuckJob> stuck = monitor.getStuckJobs();
        if (stuck.isEmpty()) {
            return Health.up().withDetail("stuckJobs", 0).build();
        }

        Health.Builder builder = Health.down().withDetail("stuckJobCount", stuck.size());
        for (QuartzStuckJobMonitor.StuckJob job : stuck) {
            builder.withDetail(job.jobName(),
                    "runtimeMs=" + job.runtimeMs() + ", trigger=" + job.triggerName() + ", fireTime=" + job.fireTime());
        }
        return builder.build();
    }
}


