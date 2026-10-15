package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.facade.MassiveSearchFacade;
import it.pagopa.cruscotto.ingestion.massivesearch.facade.SearchExecutionStartResult;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import lombok.extern.slf4j.Slf4j;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Public entry point of the Massive Search execution: enforces the per-instance concurrency rule,
 * drives the {@link ExecutionStatus} transitions of {@code search_execution} / {@code search_instance},
 * delegates the pipeline to {@link MassiveSearchEngine} and persists the latest {@code search_result}.
 *
 * <p>Each invocation produces a new latest result; no functional history is kept.</p>
 */
@Slf4j
@Service
public class MassiveSearchExecutionService implements MassiveSearchFacade {

    private final MassiveSearchProperties properties;
    private final SearchInstanceRepository instanceRepository;
    private final SearchExecutionRepository executionRepository;
    private final SearchResultRepository resultRepository;
    private final MassiveSearchEngine engine;

    public MassiveSearchExecutionService(
        MassiveSearchProperties properties,
        SearchInstanceRepository instanceRepository,
        SearchExecutionRepository executionRepository,
        SearchResultRepository resultRepository,
        MassiveSearchEngine engine
    ) {
        this.properties = properties;
        this.instanceRepository = instanceRepository;
        this.executionRepository = executionRepository;
        this.resultRepository = resultRepository;
        this.engine = engine;
    }

    @Override
    public SearchExecutionStartResult execute(UUID instanceId) {
        return run(instanceId, false);
    }

    @Override
    public SearchExecutionStartResult rerun(UUID instanceId) {
        return run(instanceId, true);
    }

    private SearchExecutionStartResult run(UUID instanceId, boolean rerun) {
        // MdcScope e non MDC.put/remove: questo metodo e' chiamato dallo scanner, che ha gia'
        // impostato entityName e runId. Cancellandoli all'uscita gli si toglieva il contesto per il
        // resto del suo lavoro; ora vengono ripristinati.
        try (MdcScope scope = MdcScope.open()
                .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH")
                .with(LogFields.CTX_INSTANCE_ID, String.valueOf(instanceId))) {
            SearchInstanceInfo instance = instanceRepository.findById(instanceId)
                .orElseThrow(() -> new MassiveSearchExecutionException("Search instance not found: " + instanceId));

            boolean allowConcurrent = properties.getExecution().isAllowConcurrentExecutionPerInstance();
            if (!allowConcurrent) {
                if (!instanceRepository.tryAcquireRunning(instanceId)) {
                    try (MdcScope ignored = MdcScope.phase("EXECUTION_LOCK_NOT_ACQUIRED");
                         LogDetails details = LogDetails.open()
                                 .with(LogFields.REASON, "not-ready-or-already-running")
                                 .with("instance_status", instance.status())) {
                        log.warn("Rejected execution, instance lock not acquired");
                    }
                    return SearchExecutionStartResult.rejected(instanceId);
                }
                // Il tentativo di acquisizione non si logga piu': era un'intenzione, e l'esito —
                // preso o non preso — e' gia' detto da una delle due righe che seguono.
                try (MdcScope ignored = MdcScope.phase("EXECUTION_LOCK_ACQUIRED")) {
                    log.debug("Acquired instance lock");
                }
            }

            UUID executionId = null;
            try {
                executionId = executionRepository.insertPending(instanceId);
                scope.with(LogFields.CTX_EXECUTION_ID, String.valueOf(executionId)).with(LogFields.CORRELATION_ID, String.valueOf(executionId));
                executionRepository.markRunning(executionId);

                MassiveSearchExecutionContext context =
                    new MassiveSearchExecutionContext(instanceId, executionId, instance.inputType(), rerun);
                context.setRequestedReports(instance.reports());
                // Collega il battito alla riga di esecuzione: da qui in poi il recoverer misura la
                // soglia sull'ultimo segno di vita e non sull'avvio, quindi un report lungo ma sano
                // non viene piu' marcato FAILED mentre sta ancora lavorando.
                UUID heartbeatExecutionId = executionId;
                context.setProgressHeartbeat(() -> writeHeartbeat(heartbeatExecutionId));
                try (MdcScope ignored = MdcScope.phase("EXECUTION_CREATED");
                     LogDetails details = LogDetails.open()
                             .with("reports", String.valueOf(instance.reports()))
                             .with("rerun", rerun)) {
                    log.info("Started search execution");
                }
                EngineResult result = engine.execute(context);

                resultRepository.upsertLatest(
                    instanceId, executionId,
                    result.zipFileName(), result.zipPath(), result.zipSizeBytes(),
                    result.positionRows(), result.tokenRows(), result.transferRows());
                long processedRows = result.positionRows() + result.tokenRows() + result.transferRows();
                executionRepository.markCompleted(executionId, result.totalInputRows(), processedRows, instance.reports().size());
                instanceRepository.markExecuted(instanceId, executionId);
                // Una riga di chiusura invece di tre (RESULT_PERSISTED, EXECUTION_COMPLETED,
                // INSTANCE_MARKED_EXECUTED): erano tre passaggi consecutivi dello stesso esito, e
                // chi legge vuole sapere se l'esecuzione e' andata a buon fine e con quante righe.
                try (MdcScope ignored = MdcScope.phase("EXECUTION_COMPLETED");
                     LogDetails details = LogDetails.open()
                             .with("blob_path", result.zipPath())
                             .with("size_bytes", result.zipSizeBytes())
                             .with("position_rows", result.positionRows())
                             .with("token_rows", result.tokenRows())
                             .with("transfer_rows", result.transferRows())) {
                    log.info("Completed search execution");
                }

                return SearchExecutionStartResult.completed(instanceId, executionId);
            } catch (RuntimeException e) {
                String errorCode = e.getClass().getSimpleName();
                if (executionId != null) {
                    executionRepository.markFailed(executionId, errorCode, e.getMessage());
                }
                // Always release the per-instance lock, even if the failure happened before the
                // execution row was created (otherwise the instance would stay RUNNING forever).
                instanceRepository.markFailed(instanceId);
                // Una riga sola: il fallimento dell'esecuzione e il rilascio del lock sono lo stesso
                // evento visto da due tabelle. execution_row_created dice se la riga di esecuzione
                // esisteva, che era l'unica differenza fra i due rami di prima.
                try (MdcScope ignored = MdcScope.phase("EXECUTION_FAILED");
                     LogDetails details = LogDetails.open()
                             .with("error_code", errorCode)
                             .with(LogFields.ERROR_DETAIL, e.getMessage())
                             .with("execution_row_created", executionId != null)) {
                    log.error("Failed search execution", e);
                }
                return SearchExecutionStartResult.failed(instanceId, executionId);
            }
        }
    }

    /**
     * Scrive il battito di presenza e segnala il caso in cui non trovi piu' l'esecuzione RUNNING.
     *
     * <p><strong>Perche' quel caso merita un WARN e non il silenzio.</strong> Significa che
     * qualcun altro ha gia' portato l'esecuzione a uno stato terminale mentre questo thread sta
     * ancora lavorando: tipicamente il recoverer su un altro pod, perche' la lease era scaduta. Da
     * quel momento il database dice FAILED e il processo dice "in corso" — i risultati che questo
     * thread produrra' non verranno piu' associati a nulla. E' l'unico punto del sistema da cui
     * quella divergenza e' osservabile: dal database si vede solo una riga FAILED del tutto
     * normale.</p>
     *
     * <p>Non interrompe l'esecuzione di proposito: fermarla a meta' lascerebbe artefatti parziali
     * sullo storage, e il lavoro gia' fatto potrebbe comunque servire a diagnosticare. La
     * strozzatura a tempo del contesto limita questa riga a una ogni 30 secondi.</p>
     */
    private void writeHeartbeat(UUID executionId) {
        if (executionRepository.heartbeat(executionId)) {
            return;
        }
        try (MdcScope ignored = MdcScope.phase("EXECUTION_NO_LONGER_RUNNING")
                     .with(LogFields.CTX_EXECUTION_ID, String.valueOf(executionId));
             LogDetails details = LogDetails.open()
                     .with(LogFields.REASON, "heartbeat-on-terminal-execution")) {
            log.warn("Kept working on an execution already moved to a terminal state");
        }
    }
}
