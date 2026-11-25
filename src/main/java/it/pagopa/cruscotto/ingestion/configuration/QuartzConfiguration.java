package it.pagopa.cruscotto.ingestion.configuration;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzPositionImportJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzPositionTokensImportJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzPositionTransfersImportJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzExtraInfoImportJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzEventsWfImportJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzAnagDescriptionImportJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzReconciliationImportJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzTokenRegistryCleanupJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzStagingErrorCleanupJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzBatchMetadataCleanupJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzStatisticsRefreshJob;
import it.pagopa.cruscotto.ingestion.scheduler.QuartzExecutionLogCleanupJob;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.scheduler.QuartzFdrDailyJob;
import it.pagopa.cruscotto.ingestion.fdr.scheduler.QuartzFdrCommandPollerJob;
import it.pagopa.cruscotto.ingestion.fdr.scheduler.QuartzFdrRetryPollerJob;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class QuartzConfiguration {

    private final IngestionConfig ingestionConfig;
    private final DbSchemaConfig dbSchemaConfig;
    private final AppModeProperties appModeProperties;
    private final FdrProperties fdrProperties;

    @Value("${ingestion.executionLog.enabled:true}")
    private boolean executionLogCleanupEnabled;

    @Value("${ingestion.executionLog.cleanupCron:0 0 2 * * ?}")
    private String executionLogCleanupCron;


    @Bean
//    @DependsOn("liquibase")
    public SchedulerFactoryBean schedulerFactoryBean(DataSource dataSource, ApplicationContext applicationContext) {

        SchedulerFactoryBean factory = new SchedulerFactoryBean();
        factory.setDataSource(dataSource);
        factory.setQuartzProperties(quartzProperties());
        factory.setOverwriteExistingJobs(true);
        boolean schedulerShouldStart = appModeProperties.schedulerShouldStart();
        factory.setAutoStartup(schedulerShouldStart);

        if (!schedulerShouldStart) {
            try (LogDetails ignored = LogDetails.open()
                    .with("mode", String.valueOf(appModeProperties.getMode()))
                    .with("scheduler_enabled", appModeProperties.isSchedulerEnabled())) {
                log.warn("Quartz scheduler startup disabled, nothing to run");
            }
            return factory;
        }

        // The job factory is always required: the Massive Search scanner is registered on this same
        // shared scheduler, so beans must be autowirable even when the ingestion jobs are disabled.
        AutowiringSpringBeanJobFactory jobFactory = new AutowiringSpringBeanJobFactory();
        jobFactory.setApplicationContext(applicationContext);
        factory.setJobFactory(jobFactory);

        if (!appModeProperties.ingestionActive()) {
            // MASSIVE_SEARCH-only: start the scheduler with no ingestion job/trigger. The scanner is
            // added afterwards by MassiveSearchSchedulerConfiguration.
            try (LogDetails ignored = LogDetails.open()
                    .with("mode", String.valueOf(appModeProperties.getMode()))
                    .with("ingestion_jobs", false)) {
                log.info("Quartz scheduler starting");
            }
            return factory;
        }

        try (LogDetails ignored = LogDetails.open()
                .with("mode", String.valueOf(appModeProperties.getMode()))
                .with("ingestion_jobs", true)) {
            log.info("Quartz scheduler starting");
        }
        factory.setJobDetails(buildJobDetails().toArray(new JobDetail[0]));

        List<Trigger> triggers = buildTriggers();
        if (!triggers.isEmpty()) {
            factory.setTriggers(triggers.toArray(new Trigger[0]));
        }

        return factory;

    }




    /**
     * I job registrati sullo scheduler.
     *
     * <p>Estratto dal metodo di factory perche' sia verificabile senza far partire Quartz: quale
     * job sia pianificato e con che cron e' una decisione di configurazione, e le decisioni di
     * configurazione si rompono in silenzio.
     */
    List<JobDetail> buildJobDetails() {
        List<JobDetail> jobDetails = new ArrayList<>(List.of(
                positionImportJobDetail(),
                positionTokensImportJobDetail(),
                positionTransfersImportJobDetail(),
                extraInfoImportJobDetail(),
                eventsWfImportJobDetail(),
                anagDescriptionImportJobDetail(),
                reconciliationJobDetail(),
                tokenRegistryCleanupJobDetail(),
                stagingErrorCleanupJobDetail(),
                batchMetadataCleanupJobDetail(),
                statisticsRefreshJobDetail(),
                executionLogCleanupJobDetail()
        ));
        // I job FDR sono registrati solo a feature accesa. Un JobDetail durevole senza trigger non
        // farebbe danni di per se', ma resterebbe scritto in QRTZ_JOB_DETAILS su ogni ambiente,
        // facendo sembrare pianificato qualcosa che non lo e'.
        if (fdrProperties.isEnabled()) {
            jobDetails.add(fdrDailyJobDetail());
            jobDetails.add(fdrCommandPollerJobDetail());
            jobDetails.add(fdrRetryPollerJobDetail());
        }

        return jobDetails;
    }

    /** I trigger attivi, dati gli interruttori e i cron configurati. */
    List<Trigger> buildTriggers() {
        List<Trigger> triggers = new ArrayList<>();
        addTriggerIfEnabled(triggers, EntityName.POSITION, positionImportJobDetail(), "positionImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.POSITION_TOKENS, positionTokensImportJobDetail(), "positionTokensImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.POSITION_TRANSFERS, positionTransfersImportJobDetail(), "positionTransfersImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.EXTRA_INFO, extraInfoImportJobDetail(), "extraInfoImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.EVENTS_WF, eventsWfImportJobDetail(), "eventsWfImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.ANAG_DESCRIPTION_REFRESH, anagDescriptionImportJobDetail(), "anagDescriptionImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.RECONCILIATION, reconciliationJobDetail(), "reconciliationTrigger");
        addStandaloneTriggerIfEnabled(triggers, ingestionConfig.getTokenRegistryCleanup().isEnabled(),
                ingestionConfig.getTokenRegistryCleanup().getCron(),
                tokenRegistryCleanupJobDetail(),
                "tokenRegistryCleanupTrigger",
                "ingestion.token-registry-cleanup.cron");
        addStandaloneTriggerIfEnabled(triggers, ingestionConfig.getStagingErrorCleanup().isEnabled(),
                ingestionConfig.getStagingErrorCleanup().getCron(),
                stagingErrorCleanupJobDetail(),
                "stagingErrorCleanupTrigger",
                "ingestion.staging-error-cleanup.cron");
        addStandaloneTriggerIfEnabled(triggers, ingestionConfig.getBatchMetadataCleanup().isEnabled(),
                ingestionConfig.getBatchMetadataCleanup().getCron(),
                batchMetadataCleanupJobDetail(),
                "batchMetadataCleanupTrigger",
                "ingestion.batch-metadata-cleanup.cron");
        addStandaloneTriggerIfEnabled(triggers, ingestionConfig.getStatisticsRefresh().isEnabled(),
                ingestionConfig.getStatisticsRefresh().getCron(),
                statisticsRefreshJobDetail(),
                "statisticsRefreshTrigger",
                "ingestion.statistics-refresh.cron");
        addStandaloneTriggerIfEnabled(triggers, executionLogCleanupEnabled,
                executionLogCleanupCron,
                executionLogCleanupJobDetail(),
                "executionLogCleanupTrigger",
                "ingestion.executionLog.cleanupCron");

        // FDR. Ogni trigger e' condizionato DUE volte: al master switch della feature e al proprio
        // interruttore. Senza il primo, un ambiente che non ha mai sentito parlare di FDR si
        // troverebbe tre job pianificati su bean che non esistono.
        addStandaloneTriggerIfEnabled(triggers,
                fdrProperties.isEnabled() && fdrProperties.getDaily().isEnabled(),
                fdrProperties.getDaily().getCron(), fdrDailyJobDetail(),
                "fdrDailyTrigger", "fdr.daily.cron",
                fdrProperties.getDaily().getTimezone());
        addStandaloneTriggerIfEnabled(triggers,
                fdrProperties.isEnabled() && fdrProperties.getCommand().isEnabled(),
                fdrProperties.getCommand().getCron(), fdrCommandPollerJobDetail(),
                "fdrCommandPollerTrigger", "fdr.command.cron",
                fdrProperties.getDaily().getTimezone());
        addStandaloneTriggerIfEnabled(triggers,
                fdrProperties.isEnabled() && fdrProperties.getRetry().isPollEnabled(),
                fdrProperties.getRetry().getPollCron(), fdrRetryPollerJobDetail(),
                "fdrRetryPollerTrigger", "fdr.retry.poll-cron",
                fdrProperties.getDaily().getTimezone());

        return triggers;
    }

    @Bean
    public Properties quartzProperties() {
        Properties props = new Properties();

        props.setProperty(
                "org.quartz.jobStore.driverDelegateClass",
                "org.quartz.impl.jdbcjobstore.PostgreSQLDelegate"
        );

        props.setProperty(
                "org.quartz.jobStore.tablePrefix",
                dbSchemaConfig.getSchemaName() + ".QRTZ_"
        );

        props.setProperty(
                "org.quartz.jobStore.isClustered",
                "true"
        );

        // In a clustered deployment every node MUST expose a distinct instanceId, otherwise all
        // pods register in QRTZ_SCHEDULER_STATE/QRTZ_FIRED_TRIGGERS under the default id and the
        // cluster manager cannot tell nodes apart: recovery on restart can re-fire triggers still
        // running on another pod (double execution) and dead-node detection stops working. AUTO
        // lets Quartz derive a unique id (hostname + timestamp) per pod.
        props.setProperty(
                "org.quartz.scheduler.instanceId",
                "AUTO"
        );

        props.setProperty(
                "org.quartz.threadPool.threadCount",
                String.valueOf(Math.max(1, ingestionConfig.getQuartz().getThreadCount()))
        );

        return props;
    }



    @Bean
    public JobDetail positionImportJobDetail() {
        return JobBuilder.newJob(QuartzPositionImportJob.class)
                .withIdentity("positionImportJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail positionTokensImportJobDetail() {
        return JobBuilder.newJob(QuartzPositionTokensImportJob.class)
                .withIdentity("positionTokensImportJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail positionTransfersImportJobDetail() {
        return JobBuilder.newJob(QuartzPositionTransfersImportJob.class)
                .withIdentity("positionTransfersImportJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail extraInfoImportJobDetail() {
        return JobBuilder.newJob(QuartzExtraInfoImportJob.class)
                .withIdentity("extraInfoImportJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail eventsWfImportJobDetail() {
        return JobBuilder.newJob(QuartzEventsWfImportJob.class)
                .withIdentity("eventsWfImportJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail anagDescriptionImportJobDetail() {
        return JobBuilder.newJob(QuartzAnagDescriptionImportJob.class)
                .withIdentity("anagDescriptionImportJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail reconciliationJobDetail() {
        return JobBuilder.newJob(QuartzReconciliationImportJob.class)
                .withIdentity("reconciliationJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail tokenRegistryCleanupJobDetail() {
        return JobBuilder.newJob(QuartzTokenRegistryCleanupJob.class)
                .withIdentity("tokenRegistryCleanupJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail stagingErrorCleanupJobDetail() {
        return JobBuilder.newJob(QuartzStagingErrorCleanupJob.class)
                .withIdentity("stagingErrorCleanupJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail batchMetadataCleanupJobDetail() {
        return JobBuilder.newJob(QuartzBatchMetadataCleanupJob.class)
                .withIdentity("batchMetadataCleanupJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail statisticsRefreshJobDetail() {
        return JobBuilder.newJob(QuartzStatisticsRefreshJob.class)
                .withIdentity("statisticsRefreshJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail executionLogCleanupJobDetail() {
        return JobBuilder.newJob(QuartzExecutionLogCleanupJob.class)
                .withIdentity("executionLogCleanupJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail fdrDailyJobDetail() {
        return JobBuilder.newJob(QuartzFdrDailyJob.class)
                .withIdentity("fdrDailyJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail fdrCommandPollerJobDetail() {
        return JobBuilder.newJob(QuartzFdrCommandPollerJob.class)
                .withIdentity("fdrCommandPollerJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail fdrRetryPollerJobDetail() {
        return JobBuilder.newJob(QuartzFdrRetryPollerJob.class)
                .withIdentity("fdrRetryPollerJob")
                .storeDurably()
                .build();
    }

    private void addTriggerIfEnabled(List<Trigger> triggers, EntityName entityName, JobDetail jobDetail, String triggerIdentity) {
        IngestionConfig.JobCronConfig jobCronConfig = getJobCronConfig(entityName);
        if (!jobCronConfig.isEnabled()) {
            return;
        }

        if (jobCronConfig.getCron() == null || jobCronConfig.getCron().isBlank()) {
            throw new IllegalStateException("Missing ingestion.quartz.jobs." + entityName.name() + ".cron");
        }

        Trigger trigger = TriggerBuilder.newTrigger()
                .forJob(jobDetail)
                .withIdentity(triggerIdentity)
                .withSchedule(CronScheduleBuilder
                        .cronSchedule(jobCronConfig.getCron())
                        .withMisfireHandlingInstructionDoNothing())
                .build();
        triggers.add(trigger);
    }

    private IngestionConfig.JobCronConfig getJobCronConfig(EntityName entityName) {
        Map<EntityName, IngestionConfig.JobCronConfig> jobs = ingestionConfig.getQuartz().getJobs();
        if (jobs == null) {
            throw new IllegalStateException("Missing ingestion.quartz.jobs configuration");
        }

        IngestionConfig.JobCronConfig config = jobs.get(entityName);
        if (config == null) {
            throw new IllegalStateException("Missing ingestion.quartz.jobs." + entityName.name() + " configuration");
        }
        return config;
    }

    /**
     * Variante con fuso orario esplicito, usata dai job FDR.
     *
     * <p>Senza, Quartz usa il fuso della JVM: un cron "alle 05:00" diventerebbe allora un orario
     * che dipende dall'immagine di base del container, e cambierebbe di nascosto a ogni
     * aggiornamento. Per un'elaborazione che deve stare dopo la pubblicazione dei file e prima
     * dell'orario di lavoro, un'ora di scarto non e' un dettaglio.
     */
    private void addStandaloneTriggerIfEnabled(List<Trigger> triggers, boolean enabled, String cron,
                                               JobDetail jobDetail, String triggerIdentity,
                                               String cronConfigKey, String timezone) {
        if (!enabled) {
            return;
        }
        if (cron == null || cron.isBlank()) {
            throw new IllegalStateException("Missing " + cronConfigKey + " configuration");
        }
        CronScheduleBuilder schedule = CronScheduleBuilder.cronSchedule(cron)
                .withMisfireHandlingInstructionDoNothing();
        if (timezone != null && !timezone.isBlank()) {
            schedule = schedule.inTimeZone(java.util.TimeZone.getTimeZone(timezone));
        }
        triggers.add(TriggerBuilder.newTrigger()
                .forJob(jobDetail)
                .withIdentity(triggerIdentity)
                .withSchedule(schedule)
                .build());
    }

    /** Fuso implicito della JVM: e' il comportamento storico dei job AS-IS, lasciato invariato. */
    private void addStandaloneTriggerIfEnabled(List<Trigger> triggers, boolean enabled, String cron, JobDetail jobDetail,
                                               String triggerIdentity, String cronConfigKey) {
        addStandaloneTriggerIfEnabled(triggers, enabled, cron, jobDetail, triggerIdentity,
                cronConfigKey, null);
    }
}
