package it.pagopa.cruscotto.ingestion.scheduler;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class KustoImportBatchJob {

    @Bean
    public Job kustoImportJob(JobRepository jobRepository, Step kustoImportStep) {
        return new JobBuilder("kustoImportJob", jobRepository)
                .start(kustoImportStep)
                .build();
    }

    @Bean
    public Step kustoImportStep(JobRepository jobRepository, PlatformTransactionManager transactionManager, ScheduledDataImporter scheduledDataImporter) {
        return new StepBuilder("kustoImportStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    scheduledDataImporter.importDataFromKusto();
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }
}
