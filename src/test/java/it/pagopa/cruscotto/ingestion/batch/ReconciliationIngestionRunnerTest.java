package it.pagopa.cruscotto.ingestion.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.entity.StagingStatus;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.StagingErrorService;
import it.pagopa.cruscotto.ingestion.service.ingestion.BulkWriter;
import it.pagopa.cruscotto.ingestion.service.ingestion.EntityTransformer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReconciliationIngestionRunnerTest {

    @Mock
    private StagingErrorService stagingErrorService;

    @Mock
    private EntityTransformer entityTransformer;

    @Mock
    private BulkWriter bulkWriter;

    private ReconciliationIngestionRunner runner;

    @BeforeEach
    void setUp() {
        IngestionConfig ingestionConfig = new IngestionConfig();
        ingestionConfig.getStaging().setMaxRetries(2);
        ingestionConfig.getReconciliation().setEnabled(true);
        ingestionConfig.getReconciliation().setBatchSize(10);

        runner = new ReconciliationIngestionRunner(
                stagingErrorService,
                entityTransformer,
                bulkWriter,
                new ObjectMapper(),
                ingestionConfig
        );
    }

    @Test
    void shouldParkRecordWhenMissingForeignKeyStillFailsAtMaxRetry() throws Exception {
        StagingIngestError pending = StagingIngestError.builder()
                .id(10L)
                .entityName(EntityName.EVENTS_WF.name())
                .sourceKey("evt-1")
                .operationId("op-1")
                .payloadJson(new ObjectMapper().writeValueAsString(Map.of("NAV", "NAV-1")))
                .status(StagingStatus.PENDING)
                .retryCount(1)
                .build();

        when(stagingErrorService.fetchPending(eq(EntityName.EVENTS_WF), eq(10))).thenReturn(List.of(pending));
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TOKENS), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TRANSFERS), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EXTRA_INFO), eq(10))).thenReturn(List.of());
        when(entityTransformer.transform(any(Map.class), any(Class.class), any(RunContext.class), eq(EntityName.EVENTS_WF)))
                .thenThrow(new EntityTransformer.TransformationException("Missing required FK fkPosition"));

        JobParameters jobParameters = new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-run-1")
                .toJobParameters();

        runner.run(jobParameters);

        verify(stagingErrorService).markParked(eq(10L), eq("recon-run-1"), any(Exception.class), eq(2));
        verify(stagingErrorService, never()).markDone(eq(10L), any());
        verify(bulkWriter, never()).writeBulk(any(), any(), any());
    }
}

