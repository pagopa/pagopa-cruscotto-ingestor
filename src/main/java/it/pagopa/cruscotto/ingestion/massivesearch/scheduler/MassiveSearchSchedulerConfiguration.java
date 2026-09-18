package it.pagopa.cruscotto.ingestion.massivesearch.scheduler;

import it.pagopa.cruscotto.ingestion.configuration.AppModeProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import lombok.extern.slf4j.Slf4j;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the Massive Search scanner ({@link MassiveSearchExecutionQuartzJob}) into the shared
 * Quartz {@link Scheduler} built by the ingestion {@code QuartzConfiguration}, without modifying that
 * configuration (bounded-context isolation).
 *
 * <p>Enabled only when {@code massive-search.scheduler.enabled=true}. The cron and identities come
 * from configuration; nothing is hardcoded. The job/trigger are (re)registered idempotently at
 * startup so a changed cron is picked up on the next boot.</p>
 *
 * <p><b>Run mode:</b> the scanner is registered only when {@code app.mode} includes MASSIVE_SEARCH and
 * the master switch {@code app.scheduler-enabled} is true (see {@link AppModeProperties}). In those
 * cases the shared scheduler is guaranteed to auto-start, so the scanner fires; in INGESTOR-only mode
 * (or when the scheduler is disabled) registration is skipped.</p>
 */
@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "massive-search.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MassiveSearchSchedulerConfiguration implements SmartInitializingSingleton {

    private static final String JOB_NAME = "massiveSearchExecutionScannerJob";
    private static final String TRIGGER_NAME = "massiveSearchExecutionScannerTrigger";

    private final Scheduler scheduler;
    private final MassiveSearchProperties properties;
    private final AppModeProperties appModeProperties;

    public MassiveSearchSchedulerConfiguration(Scheduler scheduler, MassiveSearchProperties properties,
                                               AppModeProperties appModeProperties) {
        this.scheduler = scheduler;
        this.properties = properties;
        this.appModeProperties = appModeProperties;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (!appModeProperties.massiveSearchActive()) {
            log.info("Massive Search scanner not registered: mode={} schedulerEnabled={}",
                appModeProperties.getMode(), appModeProperties.isSchedulerEnabled());
            return;
        }

        String cron = properties.getScheduler().getCron();
        if (cron == null || cron.isBlank()) {
            throw new IllegalStateException("Missing massive-search.scheduler.cron configuration");
        }

        try {
            JobKey jobKey = JobKey.jobKey(JOB_NAME);
            JobDetail jobDetail = JobBuilder.newJob(MassiveSearchExecutionQuartzJob.class)
                .withIdentity(jobKey)
                .storeDurably()
                .build();
            scheduler.addJob(jobDetail, true);

            TriggerKey triggerKey = TriggerKey.triggerKey(TRIGGER_NAME);
            CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey)
                .forJob(jobKey)
                .withSchedule(CronScheduleBuilder
                    .cronSchedule(cron)
                    .withMisfireHandlingInstructionDoNothing())
                .build();

            if (scheduler.checkExists(triggerKey)) {
                scheduler.rescheduleJob(triggerKey, trigger);
                log.info("Massive Search scanner trigger rescheduled cron={} jobKey={} triggerKey={}",
                    cron, JOB_NAME, TRIGGER_NAME);
            } else {
                scheduler.scheduleJob(trigger);
                log.info("Massive Search scanner trigger scheduled cron={} jobKey={} triggerKey={}",
                    cron, JOB_NAME, TRIGGER_NAME);
            }

            if (!scheduler.isStarted()) {
                log.warn("Massive Search scanner registered but the shared Quartz scheduler is not started "
                    + "(app.scheduler-enabled=false): the scanner will NOT fire until the scheduler is enabled");
            }
        } catch (SchedulerException e) {
            throw new IllegalStateException("Failed to register the Massive Search scanner Quartz job", e);
        }
    }
}
