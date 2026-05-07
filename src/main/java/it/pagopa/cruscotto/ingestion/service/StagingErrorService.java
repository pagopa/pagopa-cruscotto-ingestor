package it.pagopa.cruscotto.ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.entity.StagingStatus;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionErrorCode;
import it.pagopa.cruscotto.ingestion.repository.StagingIngestErrorRepository;
import it.pagopa.cruscotto.ingestion.service.ingestion.BulkWriter;
import it.pagopa.cruscotto.ingestion.service.ingestion.EntityTransformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class StagingErrorService {

    private final StagingIngestErrorRepository stagingIngestErrorRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void insertError(RunContext ctx, String sourceKey, Map<String, Object> payload, Exception ex) {
        StagingIngestError stagingError = new StagingIngestError();
        stagingError.setRunId(ctx.getRunId());
        stagingError.setEntityName(ctx.getEntityName());
        stagingError.setSourceKey(sourceKey);
        stagingError.setPayloadJson(serializePayload(payload));
        stagingError.setErrorCode(resolveErrorCode(ex).name());
        stagingError.setErrorMessage(ex != null ? ex.getMessage() : null);
        stagingError.setCreatedAt(OffsetDateTime.now(ZoneOffset.UTC));
        stagingError.setStatus(StagingStatus.PENDING);
        stagingError.setRetryCount(0);
        stagingError.setLastRetryAt(null);

        stagingIngestErrorRepository.save(stagingError);
        log.error("[{}] [{}] ERROR - staged sourceKey={} error={}", ctx.getRunId(), ctx.getEntityName(), sourceKey, stagingError.getErrorMessage());
    }

    @Transactional(readOnly = true)
    public List<StagingIngestError> fetchPending(EntityName entity, int limit) {
        int pageSize = Math.max(1, limit);
        return stagingIngestErrorRepository.findPendingByEntity(entity.name(), StagingStatus.PENDING.name(), pageSize);
    }

    @Transactional
    public void markDone(Long id, String runId) {
        stagingIngestErrorRepository.findById(id).ifPresent(error -> {
            error.setStatus(StagingStatus.DONE);
            error.setRunId(runId);
            error.setLastRetryAt(OffsetDateTime.now(ZoneOffset.UTC));
            stagingIngestErrorRepository.save(error);
        });
    }

    @Transactional
    public void markRetryFailed(Long id, String runId, Exception ex) {
        stagingIngestErrorRepository.findById(id).ifPresent(error -> {
            error.setStatus(StagingStatus.PENDING);
            error.setRunId(runId);
            error.setRetryCount((error.getRetryCount() == null ? 0 : error.getRetryCount()) + 1);
            error.setLastRetryAt(OffsetDateTime.now(ZoneOffset.UTC));
            error.setErrorCode(resolveErrorCode(ex).name());
            error.setErrorMessage(ex != null ? ex.getMessage() : null);
            stagingIngestErrorRepository.save(error);
        });
    }

    /**
     * Backward-compatible API used by existing ingestion code paths.
     */
    public void logError(String runId, String entityName, String recordData, String errorMessage) {
        RunContext ctx = new RunContext(entityName, runId, Instant.now());
        insertError(ctx, null, Collections.singletonMap("raw", recordData), new RuntimeException(errorMessage));
    }

    private String serializePayload(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload == null ? Collections.emptyMap() : payload);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    private IngestionErrorCode resolveErrorCode(Exception ex) {
        if (ex == null) {
            return IngestionErrorCode.UNEXPECTED_ERROR;
        }
        if (ex instanceof EntityTransformer.TransformationException) {
            return IngestionErrorCode.TRANSFORMATION_ERROR;
        }
        if (ex instanceof BulkWriter.BulkWriteException) {
            return IngestionErrorCode.BULK_WRITE_ERROR;
        }
        if (ex instanceof IllegalArgumentException) {
            return IngestionErrorCode.VALIDATION_ERROR;
        }
        return IngestionErrorCode.UNEXPECTED_ERROR;
    }
}

