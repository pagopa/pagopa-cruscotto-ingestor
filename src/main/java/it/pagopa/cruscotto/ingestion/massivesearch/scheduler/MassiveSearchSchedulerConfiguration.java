package it.pagopa.cruscotto.ingestion.massivesearch.scheduler;

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
 * <p><b>Coupling note:</b> the shared scheduler only auto-starts when {@code ingestion.quartz.enabled=true}
 * and only wires Spring dependencies into jobs in that case. When ingestion Quartz is disabled the
 * scanner will not fire either; this is logged as a warning.</p>
 */
@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "massive-search.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MassiveSearchSchedulerConfiguration implements SmartInitializingSingleton {

    private static final String JOB_NAME = "massiveSearchExecutionScannerJob";
    private static final String TRIGGER_NAME = "massiveSearchExecutionScannerTrigger";

    private final Scheduler scheduler;
    private final MassiveSearchProperties properties;

    public MassiveSearchSchedulerConfiguration(Scheduler scheduler, MassiveSearchProperties properties) {
        this.scheduler = scheduler;
        this.properties = properties;
    }

    @Override
    public void afterSingletonsInstantiated() {
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
                    + "(ingestion.quartz.enabled=false): the scanner will NOT fire until Quartz is enabled");
            }
        } catch (SchedulerException e) {
            throw new IllegalStateException("Failed to register the Massive Search scanner Quartz job", e);
        }
    }
}
