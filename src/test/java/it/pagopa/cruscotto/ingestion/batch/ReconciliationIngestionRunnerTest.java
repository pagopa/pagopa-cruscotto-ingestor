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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class ReconciliationIngestionRunnerTest {

    @Mock
    private StagingErrorService stagingErrorService;

    @Mock
    private EntityTransformer entityTransformer;

    @Mock
    private BulkWriter bulkWriter;

    private IngestionConfig ingestionConfig;

    private ReconciliationIngestionRunner runner;

    @BeforeEach
    void setUp() {
        ingestionConfig = new IngestionConfig();
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
    void shouldSkipSensitiveExtraInfoByBlacklistDuringReconciliation() throws Exception {
        ingestionConfig.getExtraInfo().setInfoNameBlacklist(List.of("email"));

        ObjectMapper mapper = new ObjectMapper();
        StagingIngestError pending = StagingIngestError.builder()
                .id(30L)
                .entityName(EntityName.EXTRA_INFO.name())
                .sourceKey("extra-sensitive-1")
                .operationId("op-30")
                .payloadJson(mapper.writeValueAsString(Map.of(
                        "INFO_NAME", "email",
                        "INFO_VALUE", "sensitive@example.test"
                )))
                .status(StagingStatus.PENDING)
                .retryCount(0)
                .build();

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TOKENS), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TRANSFERS), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EVENTS_WF), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EXTRA_INFO), eq(10))).thenReturn(List.of(pending));

        JobParameters jobParameters = new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-run-sensitive")
                .toJobParameters();

        runner.run(jobParameters);

        verify(stagingErrorService).markDone(30L, "recon-run-sensitive");
        verify(entityTransformer, never()).transform(any(Map.class), any(Class.class), any(RunContext.class), eq(EntityName.EXTRA_INFO));
        verify(bulkWriter, never()).writeBulk(any(), any(), any(), any());
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
        verify(bulkWriter, never()).writeBulk(any(), any(), any(), any());
    }

    @Test
    void shouldSplitAdditionalInfoForExtraInfoDuringReconciliation() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        StagingIngestError pending = StagingIngestError.builder()
                .id(20L)
                .entityName(EntityName.EXTRA_INFO.name())
                .sourceKey("extra-1")
                .operationId("op-20")
                .payloadJson(mapper.writeValueAsString(Map.of(
                        "TOKEN", "token-123",
                        "ADDITIONAL_INFO", "{\"status\":\"PAID\",\"attempts\":2}"
                )))
                .status(StagingStatus.PENDING)
                .retryCount(0)
                .build();

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TOKENS), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TRANSFERS), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EVENTS_WF), eq(10))).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EXTRA_INFO), eq(10))).thenReturn(List.of(pending));

        List<Map<String, Object>> transformedInputPayloads = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) invocation.getArgument(0);
            transformedInputPayloads.add(payload);
            return new Object();
        }).when(entityTransformer).transform(any(Map.class), any(Class.class), any(RunContext.class), eq(EntityName.EXTRA_INFO));

        JobParameters jobParameters = new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-run-extra")
                .toJobParameters();

        runner.run(jobParameters);

        assertEquals(2, transformedInputPayloads.size());
        Set<String> infoNames = transformedInputPayloads.stream()
                .map(p -> String.valueOf(p.get("INFO_NAME")))
                .collect(java.util.stream.Collectors.toSet());
        Set<String> infoValues = transformedInputPayloads.stream()
                .map(p -> String.valueOf(p.get("INFO_VALUE")))
                .collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of("status", "attempts"), infoNames);
        assertEquals(Set.of("PAID", "2"), infoValues);

        verify(entityTransformer, atLeastOnce()).transform(any(Map.class), any(Class.class), any(RunContext.class), eq(EntityName.EXTRA_INFO));

        ArgumentCaptor<List> batchCaptor = ArgumentCaptor.forClass(List.class);
        verify(bulkWriter).writeBulk(eq(EntityName.EXTRA_INFO), batchCaptor.capture(), eq("recon-run-extra"), any());
        assertEquals(2, batchCaptor.getValue().size());

        verify(stagingErrorService).markDone(20L, "recon-run-extra");
    }
}

