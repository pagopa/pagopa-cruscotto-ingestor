package it.pagopa.cruscotto.ingestion.configuration;

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
import it.pagopa.cruscotto.ingestion.scheduler.QuartzExecutionLogCleanupJob;
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
        factory.setAutoStartup(ingestionConfig.getQuartz().isEnabled());

        if (!ingestionConfig.getQuartz().isEnabled()) {
            log.warn("Quartz scheduler startup is disabled by configuration ingestion.quartz.enabled=false");
            return factory;
        }

        AutowiringSpringBeanJobFactory jobFactory = new AutowiringSpringBeanJobFactory();
        jobFactory.setApplicationContext(applicationContext);
        factory.setJobFactory(jobFactory);

        factory.setJobDetails(
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
                executionLogCleanupJobDetail()
        );

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
        addStandaloneTriggerIfEnabled(triggers, executionLogCleanupEnabled,
                executionLogCleanupCron,
                executionLogCleanupJobDetail(),
                "executionLogCleanupTrigger",
                "ingestion.executionLog.cleanupCron");

        if (!triggers.isEmpty()) {
            factory.setTriggers(triggers.toArray(new Trigger[0]));
        }

        return factory;

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
    public JobDetail executionLogCleanupJobDetail() {
        return JobBuilder.newJob(QuartzExecutionLogCleanupJob.class)
                .withIdentity("executionLogCleanupJob")
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

    private void addStandaloneTriggerIfEnabled(List<Trigger> triggers, boolean enabled, String cron, JobDetail jobDetail,
                                               String triggerIdentity, String cronConfigKey) {
        if (!enabled) {
            return;
        }
        if (cron == null || cron.isBlank()) {
            throw new IllegalStateException("Missing " + cronConfigKey + " configuration");
        }
        Trigger trigger = TriggerBuilder.newTrigger()
                .forJob(jobDetail)
                .withIdentity(triggerIdentity)
                .withSchedule(CronScheduleBuilder
                        .cronSchedule(cron)
                        .withMisfireHandlingInstructionDoNothing())
                .build();
        triggers.add(trigger);
    }
}
