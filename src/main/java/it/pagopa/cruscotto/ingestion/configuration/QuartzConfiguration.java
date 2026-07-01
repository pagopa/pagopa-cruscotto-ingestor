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
import lombok.RequiredArgsConstructor;
import org.quartz.*;
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
@RequiredArgsConstructor
public class QuartzConfiguration {

    private final IngestionConfig ingestionConfig;
    private final DbSchemaConfig dbSchemaConfig;


    @Bean
//    @DependsOn("liquibase")
    public SchedulerFactoryBean schedulerFactoryBean(DataSource dataSource, ApplicationContext applicationContext) {

        SchedulerFactoryBean factory = new SchedulerFactoryBean();
        factory.setDataSource(dataSource);
        factory.setQuartzProperties(quartzProperties());
        factory.setOverwriteExistingJobs(true);

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
                reconciliationJobDetail()
        );

        List<Trigger> triggers = new ArrayList<>();
        addTriggerIfEnabled(triggers, EntityName.POSITION, positionImportJobDetail(), "positionImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.POSITION_TOKENS, positionTokensImportJobDetail(), "positionTokensImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.POSITION_TRANSFERS, positionTransfersImportJobDetail(), "positionTransfersImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.EXTRA_INFO, extraInfoImportJobDetail(), "extraInfoImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.EVENTS_WF, eventsWfImportJobDetail(), "eventsWfImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.ANAG_DESCRIPTION_REFRESH, anagDescriptionImportJobDetail(), "anagDescriptionImportTrigger");
        addTriggerIfEnabled(triggers, EntityName.RECONCILIATION, reconciliationJobDetail(), "reconciliationTrigger");

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
}
