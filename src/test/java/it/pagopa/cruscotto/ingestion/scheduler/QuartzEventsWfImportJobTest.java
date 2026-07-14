package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.CheckpointStoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.quartz.JobExecutionContext;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QuartzEventsWfImportJobTest {

    @Mock
    private JobLauncher jobLauncher;

    @Mock
    private Job eventsWfImportJob;

    @Mock
    private CheckpointStoreService checkpointStoreService;

    @Mock
    private JobExecutionContext jobExecutionContext;

    private QuartzEventsWfImportJob quartzEventsWfImportJob;

    @BeforeEach
    void setUp() {
        quartzEventsWfImportJob = new QuartzEventsWfImportJob();
        IngestionConfig ingestionConfig = new IngestionConfig();
        ingestionConfig.getEventsWf().getDedicated().setEnabled(true);
        ingestionConfig.getEventsWf().getDedicated().setExtraRunsWhenBacklogged(2);
        ingestionConfig.getEventsWf().getDedicated().setBacklogThreshold(Duration.ofHours(1));

        ReflectionTestUtils.setField(quartzEventsWfImportJob, "jobLauncher", jobLauncher);
        ReflectionTestUtils.setField(quartzEventsWfImportJob, "eventsWfImportJob", eventsWfImportJob);
        ReflectionTestUtils.setField(quartzEventsWfImportJob, "ingestionConfig", ingestionConfig);
        ReflectionTestUtils.setField(quartzEventsWfImportJob, "checkpointStoreService", checkpointStoreService);

        Date now = new Date();
        when(jobExecutionContext.getScheduledFireTime()).thenReturn(now);
        when(jobExecutionContext.getNextFireTime()).thenReturn(now);
    }

    @Test
    void shouldRunDedicatedBurstWhenEventsWfIsBacklogged() throws Exception {
        when(checkpointStoreService.getCheckpoint(EntityName.POSITION_TOKENS))
                .thenReturn(Optional.of(Instant.parse("2026-07-14T10:00:00Z")));
        when(checkpointStoreService.getCheckpoint(EntityName.EVENTS_WF))
                .thenReturn(Optional.of(Instant.parse("2026-07-14T07:00:00Z")));

        quartzEventsWfImportJob.executeInternal(jobExecutionContext);

        verify(jobLauncher, times(3)).run(eq(eventsWfImportJob), any());
    }

    @Test
    void shouldRunSingleExecutionWhenNoBacklog() throws Exception {
        when(checkpointStoreService.getCheckpoint(EntityName.POSITION_TOKENS))
                .thenReturn(Optional.of(Instant.parse("2026-07-14T10:00:00Z")));
        when(checkpointStoreService.getCheckpoint(EntityName.EVENTS_WF))
                .thenReturn(Optional.of(Instant.parse("2026-07-14T09:30:00Z")));

        quartzEventsWfImportJob.executeInternal(jobExecutionContext);

        verify(jobLauncher, times(1)).run(eq(eventsWfImportJob), any());
    }
}
