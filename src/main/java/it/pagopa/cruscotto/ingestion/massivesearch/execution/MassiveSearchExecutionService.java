package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.facade.MassiveSearchFacade;
import it.pagopa.cruscotto.ingestion.massivesearch.facade.SearchExecutionStartResult;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
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

    private static final int GENERATED_REPORT_FILES = 3;

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
        MDC.put("entityName", "MASSIVE_SEARCH");
        MDC.put("instanceId", String.valueOf(instanceId));
        try {
            SearchInstanceInfo instance = instanceRepository.findById(instanceId)
                .orElseThrow(() -> new MassiveSearchExecutionException("Search instance not found: " + instanceId));

            boolean allowConcurrent = properties.getExecution().isAllowConcurrentExecutionPerInstance();
            if (!allowConcurrent) {
                log.info("phase=EXECUTION_LOCK_ACQUIRE_ATTEMPT instanceId={} status={}", instanceId, instance.status());
                if (!instanceRepository.tryAcquireRunning(instanceId)) {
                    log.warn("phase=EXECUTION_LOCK_NOT_ACQUIRED instanceId={} reason=not-ready-or-already-running", instanceId);
                    return SearchExecutionStartResult.rejected(instanceId);
                }
                log.info("phase=EXECUTION_LOCK_ACQUIRED instanceId={} status=RUNNING", instanceId);
            }

            UUID executionId = null;
            try {
                executionId = executionRepository.insertPending(instanceId);
                MDC.put("executionId", String.valueOf(executionId));
                MDC.put("runId", String.valueOf(executionId));
                executionRepository.markRunning(executionId);
                log.info("phase=EXECUTION_CREATED instanceId={} executionId={} status=RUNNING", instanceId, executionId);

                MassiveSearchExecutionContext context =
                    new MassiveSearchExecutionContext(instanceId, executionId, instance.inputType(), rerun);
                EngineResult result = engine.execute(context);

                resultRepository.upsertLatest(
                    instanceId, executionId,
                    result.zipFileName(), result.zipPath(), result.zipSizeBytes(),
                    result.positionRows(), result.attemptRows(), result.transferRows());
                log.info("phase=RESULT_PERSISTED instanceId={} executionId={} blobPath={} sizeBytes={}",
                    instanceId, executionId, result.zipPath(), result.zipSizeBytes());
                long processedRows = result.positionRows() + result.attemptRows() + result.transferRows();
                executionRepository.markCompleted(executionId, result.totalInputRows(), processedRows, GENERATED_REPORT_FILES);
                log.info("phase=EXECUTION_COMPLETED instanceId={} executionId={} status=COMPLETED positionRows={} attemptRows={} transferRows={}",
                    instanceId, executionId, result.positionRows(), result.attemptRows(), result.transferRows());
                instanceRepository.markExecuted(instanceId, executionId);
                log.info("phase=INSTANCE_MARKED_EXECUTED instanceId={} executionId={} status=EXECUTED", instanceId, executionId);

                return SearchExecutionStartResult.completed(instanceId, executionId);
            } catch (RuntimeException e) {
                String errorCode = e.getClass().getSimpleName();
                if (executionId != null) {
                    executionRepository.markFailed(executionId, errorCode, e.getMessage());
                    log.error("phase=EXECUTION_FAILED instanceId={} executionId={} status=FAILED errorCode={} reason={}",
                        instanceId, executionId, errorCode, e.getMessage(), e);
                } else {
                    log.error("phase=EXECUTION_FAILED instanceId={} executionId=null status=FAILED errorCode={} reason={}",
                        instanceId, errorCode, e.getMessage(), e);
                }
                // Always release the per-instance lock, even if the failure happened before the
                // execution row was created (otherwise the instance would stay RUNNING forever).
                instanceRepository.markFailed(instanceId);
                log.info("phase=INSTANCE_MARKED_FAILED instanceId={} executionId={} status=FAILED errorCode={}",
                    instanceId, executionId, errorCode);
                return SearchExecutionStartResult.failed(instanceId, executionId);
            }
        } finally {
            MDC.remove("runId");
            MDC.remove("executionId");
            MDC.remove("instanceId");
            MDC.remove("entityName");
        }
    }
}
