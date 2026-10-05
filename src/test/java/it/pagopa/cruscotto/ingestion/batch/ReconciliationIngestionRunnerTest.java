package it.pagopa.cruscotto.ingestion.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.entity.StagingStatus;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.ExtraInfoWhitelistService;
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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

    @Mock
    private ExtraInfoWhitelistService extraInfoWhitelistService;

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
                ingestionConfig,
                extraInfoWhitelistService
        );

        lenient().when(extraInfoWhitelistService.isAllowed(any())).thenReturn(true);
    }

    @Test
    void shouldSkipExtraInfoNotInWhitelistDuringReconciliation() throws Exception {
        when(extraInfoWhitelistService.isAllowed("email")).thenReturn(false);

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

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TOKENS), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TRANSFERS), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EVENTS_WF), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EXTRA_INFO), eq(10), any(), any())).thenReturn(List.of(pending));

        JobParameters jobParameters = new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-run-sensitive")
                .toJobParameters();

        runner.run(jobParameters);

        verify(stagingErrorService).markDone(eq(pending), eq("recon-run-sensitive"));
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

        when(stagingErrorService.fetchPending(eq(EntityName.EVENTS_WF), eq(10), any(), any())).thenReturn(List.of(pending));
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TOKENS), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TRANSFERS), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EXTRA_INFO), eq(10), any(), any())).thenReturn(List.of());
        when(entityTransformer.transform(any(Map.class), any(Class.class), any(RunContext.class), eq(EntityName.EVENTS_WF)))
                .thenThrow(new EntityTransformer.TransformationException("Missing required FK fkPosition"));

        JobParameters jobParameters = new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-run-1")
                .toJobParameters();

        runner.run(jobParameters);

        verify(stagingErrorService).markParked(eq(pending), eq("recon-run-1"), any(Exception.class), eq(2));
        verify(stagingErrorService, never()).markDone(eq(pending), any());
        verify(bulkWriter, never()).writeBulk(any(), any(), any(), any());
    }

    @Test
    void shouldIsolatePerRecordFailuresAndContinueBatch() throws Exception {
        // With the resourceless step transaction manager each writeBulk/markDone commits in its own
        // transaction, so one record's bulk-write failure must NOT abort the rest of the batch:
        // successful records still get DONE and the failed one is retried, independently.
        ObjectMapper mapper = new ObjectMapper();
        StagingIngestError r1 = positionPending(mapper, 1L, "op-1");
        StagingIngestError r2 = positionPending(mapper, 2L, "op-2");
        StagingIngestError r3 = positionPending(mapper, 3L, "op-3");

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10), any(), any())).thenReturn(List.of(r1, r2, r3));
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TOKENS), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TRANSFERS), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EVENTS_WF), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EXTRA_INFO), eq(10), any(), any())).thenReturn(List.of());

        // 1st record succeeds, 2nd fails the bulk write, 3rd succeeds again.
        doReturn(null)
                .doThrow(new BulkWriter.BulkWriteException("boom"))
                .doReturn(null)
                .when(bulkWriter).writeBulk(any(), any(), any(), any());

        JobParameters jobParameters = new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-isolation")
                .toJobParameters();

        runner.run(jobParameters);

        verify(bulkWriter, times(3)).writeBulk(any(), any(), any(), any());
        // Successful records are marked DONE despite the failure of the record between them.
        verify(stagingErrorService).markDone(eq(r1), eq("recon-isolation"));
        verify(stagingErrorService).markDone(eq(r3), eq("recon-isolation"));
        // The failed record (retryCount 0, below maxRetries 2) is scheduled for retry, not parked.
        verify(stagingErrorService).markRetryFailed(eq(r2), eq("recon-isolation"), any(BulkWriter.BulkWriteException.class));
        verify(stagingErrorService, never()).markDone(eq(r2), any());
    }

    private static StagingIngestError positionPending(ObjectMapper mapper, long id, String operationId) throws Exception {
        return StagingIngestError.builder()
                .id(id)
                .entityName(EntityName.POSITION.name())
                .sourceKey("pos-" + id)
                .operationId(operationId)
                .payloadJson(mapper.writeValueAsString(Map.of("NAV", "NAV-" + id, "PA_EMITTENTE", "PA-" + id)))
                .status(StagingStatus.PENDING)
                .retryCount(0)
                .build();
    }

    /**
     * Il drain cicla finche' i batch tornano pieni. Senza questo, una esecuzione processava
     * {@code batchSize} record per entita' e si fermava: in produzione un singolo run di ingestion ne
     * accodava centinaia di migliaia, quindi la coda non era smaltibile e i record scadevano per
     * retention invece di essere recuperati.
     */
    @Test
    void shouldKeepDrainingWhileBatchesComeBackFull() throws Exception {
        ingestionConfig.getReconciliation().setBatchSize(2);
        ObjectMapper mapper = new ObjectMapper();

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(2), any(), any()))
                .thenReturn(List.of(positionPending(mapper, 1L, "op-1"), positionPending(mapper, 2L, "op-2")))
                .thenReturn(List.of(positionPending(mapper, 3L, "op-3")));

        runner.run(new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-drain")
                .toJobParameters());

        // Secondo giro perche' il primo batch era pieno; si ferma al batch parziale, che e' il segnale
        // che non c'e' piu' arretrato.
        verify(stagingErrorService, times(2)).fetchPending(eq(EntityName.POSITION), eq(2), any(), any());
        verify(bulkWriter, times(3)).writeBulk(any(), any(), any(), any());
    }

    /**
     * Un record puo' essere tentato <strong>una volta sola per esecuzione</strong>. Il drain cicla e
     * ogni esito scrive LAST_RETRY_AT, quindi senza un bound superiore sull'ordinamento, esaurite le
     * righe mai tentate il fetch ricomincerebbe da quelle gia' tentate nello stesso giro: i 20
     * tentativi di staging.max-retries — che sono il tempo concesso all'entita' padre per arrivare da
     * ADX — si brucerebbero in pochi minuti. Il bound e' l'istante di avvio del drain, uguale per
     * tutti i fetch dell'esecuzione: e' anche cio' che garantisce la terminazione del loop.
     */
    @Test
    void shouldAttemptEachRecordAtMostOncePerExecution() throws Exception {
        ingestionConfig.getReconciliation().setBatchSize(1);
        ObjectMapper mapper = new ObjectMapper();

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(1), any(), any()))
                .thenReturn(List.of(positionPending(mapper, 1L, "op-1")))
                .thenReturn(List.of(positionPending(mapper, 2L, "op-2")))
                .thenReturn(List.of());

        OffsetDateTime beforeRun = OffsetDateTime.now(ZoneOffset.UTC);
        runner.run(new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-once")
                .toJobParameters());
        OffsetDateTime afterRun = OffsetDateTime.now(ZoneOffset.UTC);

        ArgumentCaptor<OffsetDateTime> triedBefore = ArgumentCaptor.forClass(OffsetDateTime.class);
        verify(stagingErrorService, times(3))
                .fetchPending(eq(EntityName.POSITION), eq(1), any(), triedBefore.capture());
        List<OffsetDateTime> bounds = triedBefore.getAllValues();
        // Lo stesso istante per tutti i fetch: un bound ricalcolato a ogni giro riammetterebbe i
        // record appena tentati, che e' esattamente cio' da evitare.
        assertEquals(1, Set.copyOf(bounds).size());
        assertThat(bounds.get(0)).isAfterOrEqualTo(beforeRun).isBeforeOrEqualTo(afterRun);
    }

    /**
     * Il drain non puo' girare senza limite: il job si sovrapporrebbe alla propria esecuzione
     * successiva e competerebbe con l'ingestion per le connessioni. Il progresso non va perso, ogni
     * record e' chiuso nella propria transazione.
     */
    @Test
    void shouldStopAtTheDurationBudgetLeavingTheRestToTheNextRun() throws Exception {
        ingestionConfig.getReconciliation().setBatchSize(1);
        ingestionConfig.getReconciliation().setMaxDuration(java.time.Duration.ofMillis(1));
        ObjectMapper mapper = new ObjectMapper();

        // Ogni batch torna pieno: senza il tetto il loop non terminerebbe mai.
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(1), any(), any()))
                .thenReturn(List.of(positionPending(mapper, 1L, "op-1")));
        doAnswer(invocation -> {
            Thread.sleep(5);
            return null;
        }).when(bulkWriter).writeBulk(any(), any(), any(), any());

        runner.run(new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-budget")
                .toJobParameters());

        verify(stagingErrorService, times(1)).fetchPending(eq(EntityName.POSITION), eq(1), any(), any());
    }

    /**
     * Il budget interrompe anche <strong>a meta' batch</strong>. Un giro del drain vale fino a
     * batch-size record per ciascuna delle cinque entita': verificandolo solo fra i giri, con un DB
     * lento (statement_timeout a 60s per batch) si sforerebbe di ore. Interrompere a meta' e' sicuro
     * perche' ogni record e' chiuso nella propria transazione.
     */
    @Test
    void shouldStopMidBatchWhenTheBudgetRunsOut() throws Exception {
        ingestionConfig.getReconciliation().setBatchSize(10);
        ingestionConfig.getReconciliation().setMaxDuration(java.time.Duration.ofMillis(20));
        ObjectMapper mapper = new ObjectMapper();

        List<StagingIngestError> batch = new ArrayList<>();
        for (long id = 1; id <= 10; id++) {
            batch.add(positionPending(mapper, id, "op-" + id));
        }
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10), any(), any()))
                .thenReturn(batch);
        doAnswer(invocation -> {
            Thread.sleep(10);
            return null;
        }).when(bulkWriter).writeBulk(any(), any(), any(), any());

        runner.run(new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-midbatch")
                .toJobParameters());

        // Fermato dentro il batch: meno dei 10 record scritti, e nessun secondo fetch.
        verify(bulkWriter, atMost(5)).writeBulk(any(), any(), any(), any());
        verify(stagingErrorService, times(1)).fetchPending(eq(EntityName.POSITION), eq(10), any(), any());
    }

    /**
     * Un {@code max-duration} troppo basso non deve produrre uno stallo silenzioso: un'esecuzione
     * tenta sempre almeno un record, altrimenti un errore di configurazione bloccherebbe la coda per
     * sempre invece di rallentarla.
     *
     * <p>Il valore sub-millisecondo e' voluto: {@code toMillis()} lo troncherebbe a 0, che nel
     * predicato significa <em>tetto disattivato</em> — quindi girerebbe l'intero batch invece di
     * fermarsi. E' la stessa trappola gia' vista su {@code statement-timeout}.</p>
     */
    @Test
    void shouldAlwaysAttemptAtLeastOneRecordEvenWithAnImpossibleBudget() throws Exception {
        ingestionConfig.getReconciliation().setBatchSize(10);
        ingestionConfig.getReconciliation().setMaxDuration(java.time.Duration.ofNanos(1));
        ObjectMapper mapper = new ObjectMapper();

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10), any(), any()))
                .thenReturn(List.of(positionPending(mapper, 1L, "op-1"), positionPending(mapper, 2L, "op-2")));

        runner.run(new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-impossible")
                .toJobParameters());

        verify(bulkWriter, times(1)).writeBulk(any(), any(), any(), any());
    }

    /** Un tetto a zero disattiva il limite di durata, come per la retention dello staging. */
    @Test
    void aZeroBudgetMeansNoTimeLimit() throws Exception {
        ingestionConfig.getReconciliation().setBatchSize(1);
        ingestionConfig.getReconciliation().setMaxDuration(java.time.Duration.ZERO);
        ObjectMapper mapper = new ObjectMapper();

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(1), any(), any()))
                .thenReturn(List.of(positionPending(mapper, 1L, "op-1")))
                .thenReturn(List.of(positionPending(mapper, 2L, "op-2")))
                .thenReturn(List.of());

        runner.run(new JobParametersBuilder()
                .addString(JobParameterKeys.RUN_ID, "recon-no-budget")
                .toJobParameters());

        verify(stagingErrorService, times(3)).fetchPending(eq(EntityName.POSITION), eq(1), any(), any());
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

        when(stagingErrorService.fetchPending(eq(EntityName.POSITION), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TOKENS), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.POSITION_TRANSFERS), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EVENTS_WF), eq(10), any(), any())).thenReturn(List.of());
        when(stagingErrorService.fetchPending(eq(EntityName.EXTRA_INFO), eq(10), any(), any())).thenReturn(List.of(pending));

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

        verify(stagingErrorService).markDone(eq(pending), eq("recon-run-extra"));
    }
}
