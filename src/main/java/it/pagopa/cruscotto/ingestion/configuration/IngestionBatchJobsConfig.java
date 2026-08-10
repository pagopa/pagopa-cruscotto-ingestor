package it.pagopa.cruscotto.ingestion.configuration;

import it.pagopa.cruscotto.ingestion.batch.*;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class IngestionBatchJobsConfig {

    private final JobRepository jobRepository;

    /**
     * Transaction manager used for ALL ingestion tasklet steps (ADX import, anag-description
     * refresh and reconciliation).
     * <p>
     * Spring Batch wraps each tasklet execution in a transaction managed by the step's
     * transaction manager. Because these steps perform long ADX reads (up to the ADX query
     * timeout) inside the tasklet, using the JPA/JDBC transaction manager here would keep a
     * Postgres connection open "idle in transaction" for the whole run (blocking autovacuum and
     * wasting a pooled connection). All actual DB writes already open their own transactions
     * ({@code REQUIRES_NEW} in WindowCyclePersistenceService / ExecutionLogService /
     * CheckpointStoreService and {@code @Transactional} in BulkWriterImpl / StagingErrorService),
     * so the step itself does not need a DB-backed transaction. Using a resourceless manager means
     * a connection is held only during the real writes, and each write commits independently
     * (avoiding a single per-batch rollback-only wiping the whole reconciliation batch).
     */
    private final ResourcelessTransactionManager ingestionStepTransactionManager =
            new ResourcelessTransactionManager();
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
                }, ingestionStepTransactionManager)
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
                }, ingestionStepTransactionManager)
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
                }, ingestionStepTransactionManager)
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
                }, ingestionStepTransactionManager)
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
                }, ingestionStepTransactionManager)
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
                }, ingestionStepTransactionManager)
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
                }, ingestionStepTransactionManager)
                .build();
    }
}
