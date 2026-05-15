package it.pagopa.cruscotto.ingestion.batch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.StagingErrorService;
import it.pagopa.cruscotto.ingestion.service.ingestion.BulkWriter;
import it.pagopa.cruscotto.ingestion.service.ingestion.EntityTransformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReconciliationIngestionRunner {

    private final StagingErrorService stagingErrorService;
    private final EntityTransformer entityTransformer;
    private final BulkWriter bulkWriter;
    private final ObjectMapper objectMapper;
    private final IngestionConfig ingestionConfig;

    public void run(JobParameters jobParameters) {
        String runId = jobParameters.getString(JobParameterKeys.RUN_ID);
        int maxRetries = ingestionConfig.getStaging().getMaxRetries();

        if (!ingestionConfig.getReconciliation().isEnabled()) {
            log.info("[runId={}][phase=NOOP] Reconciliation disabled via config", runId);
            return;
        }

        int batchSize = ingestionConfig.getReconciliation().getBatchSize();

        for (EntityName entity : EntityName.values()) {
            if (!hasPendingRecords(entity)) {
                continue;
            }

            List<StagingIngestError> pending = stagingErrorService.fetchPending(entity, batchSize);            if (pending.isEmpty()) {
                continue;
            }

            log.info("[runId={}][entity={}][phase=START] pendingCount={}", runId, entity.name(), pending.size());

            for (StagingIngestError record : pending) {
                // Verificare se il record ha superato il limite di retry
                int currentRetryCount = record.getRetryCount() != null ? record.getRetryCount() : 0;
                if (currentRetryCount >= maxRetries) {
                    stagingErrorService.markParked(record.getId(), runId,
                            new IllegalStateException("Record parked after exhausting reconciliation retries"),
                            currentRetryCount);
                    log.error("[runId={}][entity={}][phase=ERROR] stagingId={} retryCount={} maxRetries={} marked=PARKED",
                            runId, entity.name(), record.getId(), currentRetryCount, maxRetries);
                    continue;
                }

                try {
                    Map<String, Object> payload = objectMapper.readValue(
                            record.getPayloadJson(),
                            new TypeReference<>() {}
                    );

                    RunContext ctx = new RunContext(entity.name(), runId, Instant.now());
                    ctx.setOperationId(record.getOperationId());

                    Object transformed = entityTransformer.transform(payload, getTargetClass(entity), ctx, entity);

                    // Errori di trasformazione (dominio) → staging via exception handler
                    // Successo bulk → DONE
                    bulkWriter.writeBulk(entity, List.of(transformed), runId);
                    stagingErrorService.markDone(record.getId(), runId);

                    log.info("[runId={}][entity={}][phase=BULK_OK] stagingId={} marked=DONE",
                            runId, entity.name(), record.getId());

                } catch (BulkWriter.BulkWriteException e) {
                    int nextRetryCount = currentRetryCount + 1;
                    if (nextRetryCount >= maxRetries) {
                        stagingErrorService.markParked(record.getId(), runId, e, nextRetryCount);
                        log.error("[runId={}][entity={}][phase=BULK_KO_TOTAL] stagingId={} retryCount={} maxRetries={} marked=PARKED error={}",
                                runId, entity.name(), record.getId(), nextRetryCount, maxRetries, e.getMessage());
                    } else {
                        stagingErrorService.markRetryFailed(record.getId(), runId, e);
                        log.error("[runId={}][entity={}][phase=BULK_KO_TOTAL] stagingId={} retryCount={} error={}",
                                runId, entity.name(), record.getId(), nextRetryCount, e.getMessage());
                    }

                } catch (Exception ex) {
                    int nextRetryCount = currentRetryCount + 1;
                    if (nextRetryCount >= maxRetries) {
                        stagingErrorService.markParked(record.getId(), runId, ex, nextRetryCount);
                        log.error("[runId={}][entity={}][phase=ERROR] stagingId={} sourceKey={} retryCount={} maxRetries={} marked=PARKED message={}",
                                runId, entity.name(), record.getId(), record.getSourceKey(), nextRetryCount, maxRetries, ex.getMessage());
                    } else {
                        stagingErrorService.markRetryFailed(record.getId(), runId, ex);
                        log.error("[runId={}][entity={}][phase=ERROR] stagingId={} sourceKey={} retryCount={} message={}",
                                runId, entity.name(), record.getId(), record.getSourceKey(), nextRetryCount, ex.getMessage());
                    }
                }
            }

            log.info("[runId={}][entity={}][phase=END] processedCount={}", runId, entity.name(), pending.size());
        }
    }

    private boolean hasPendingRecords(EntityName entity) {
        try {
            // Filtrare solo le entità gestite; le entità senza classe target vengono saltate silenziosamente
            getTargetClass(entity);
            return true;
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private Class<?> getTargetClass(EntityName entityName) {
        return switch (entityName) {
            case POSITION -> Position.class;
            case POSITION_TOKENS -> PositionTokens.class;
            case POSITION_TRANSFERS -> PositionTransfers.class;
            case EXTRA_INFO -> ExtraInfo.class;
            case EVENTS_WF -> EventsWf.class;
            default -> throw new IllegalArgumentException("No reconciliation target class configured for entity: " + entityName);
        };
    }
}







