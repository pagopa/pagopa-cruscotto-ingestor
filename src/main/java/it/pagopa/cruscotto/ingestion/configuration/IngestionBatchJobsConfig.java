package it.pagopa.cruscotto.ingestion.configuration;

import it.pagopa.cruscotto.ingestion.batch.*;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class IngestionBatchJobsConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final PositionIngestionRunner positionIngestionRunner;
    private final PositionTokensIngestionRunner positionTokensIngestionRunner;
    private final PositionTransfersIngestionRunner positionTransfersIngestionRunner;
    private final ExtraInfoIngestionRunner extraInfoIngestionRunner;
    private final EventsWfIngestionRunner eventsWfIngestionRunner;
    private final AnagDescriptionIngestionRunner anagDescriptionIngestionRunner;
    private final ReconciliationIngestionRunner reconciliationIngestionRunner;

    // POSITION entity
    @Bean
    public Job positionImportJob() {
        return new JobBuilder("positionImportJob", jobRepository)
                .start(positionImportStep())
                .build();
    }

    @Bean
    public Step positionImportStep() {
        return new StepBuilder("positionImportStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    positionIngestionRunner.run(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    // POSITION_TOKENS entity
    @Bean
    public Job positionTokensImportJob() {
        return new JobBuilder("positionTokensImportJob", jobRepository)
                .start(positionTokensImportStep())
                .build();
    }

    @Bean
    public Step positionTokensImportStep() {
        return new StepBuilder("positionTokensImportStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    positionTokensIngestionRunner.run(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    // POSITION_TRANSFERS entity
    @Bean
    public Job positionTransfersImportJob() {
        return new JobBuilder("positionTransfersImportJob", jobRepository)
                .start(positionTransfersImportStep())
                .build();
    }

    @Bean
    public Step positionTransfersImportStep() {
        return new StepBuilder("positionTransfersImportStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    positionTransfersIngestionRunner.run(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    // EXTRA_INFO entity
    @Bean
    public Job extraInfoImportJob() {
        return new JobBuilder("extraInfoImportJob", jobRepository)
                .start(extraInfoImportStep())
                .build();
    }

    @Bean
    public Step extraInfoImportStep() {
        return new StepBuilder("extraInfoImportStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    extraInfoIngestionRunner.run(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    // EVENTS_WF entity
    @Bean
    public Job eventsWfImportJob() {
        return new JobBuilder("eventsWfImportJob", jobRepository)
                .start(eventsWfImportStep())
                .build();
    }

    @Bean
    public Step eventsWfImportStep() {
        return new StepBuilder("eventsWfImportStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    eventsWfIngestionRunner.run(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Job anagDescriptionImportJob() {
        return new JobBuilder("anagDescriptionImportJob", jobRepository)
                .start(anagDescriptionImportStep())
                .build();
    }

    @Bean
    public Step anagDescriptionImportStep() {
        return new StepBuilder("anagDescriptionImportStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    anagDescriptionIngestionRunner.run(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Job reconciliationJob() {
        return new JobBuilder("reconciliationJob", jobRepository)
                .start(reconciliationStep())
                .build();
    }

    @Bean
    public Step reconciliationStep() {
        return new StepBuilder("reconciliationStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    reconciliationIngestionRunner.run(chunkContext.getStepContext().getStepExecution().getJobExecution().getJobParameters());
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }
}
