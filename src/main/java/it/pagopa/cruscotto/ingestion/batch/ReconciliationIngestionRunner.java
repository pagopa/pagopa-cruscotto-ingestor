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

        for (EntityName entity : EntityName.values()) {
            List<StagingIngestError> pending = stagingErrorService.fetchPending(entity, ingestionConfig.getReconciliationFetchLimit());
            if (pending.isEmpty()) {
                continue;
            }

            log.info("START runId={} entityName={} pendingCount={}", runId, entity.name(), pending.size());

            for (StagingIngestError record : pending) {
                try {
                    Map<String, Object> payload = objectMapper.readValue(
                            record.getPayloadJson(),
                            new TypeReference<Map<String, Object>>() {
                            }
                    );

                    Object transformed = entityTransformer.transform(payload, getTargetClass(entity));
                    bulkWriter.writeBulk(entity, List.of(transformed));
                    stagingErrorService.markDone(record.getId(), runId);
                } catch (Exception ex) {
                    stagingErrorService.markRetryFailed(record.getId(), runId, ex);
                    log.error("ERROR runId={} entityName={} sourceKey={} id={} message={}",
                            runId,
                            entity.name(),
                            record.getSourceKey(),
                            record.getId(),
                            ex.getMessage());
                    // Keep processing next records.
                }
            }

            log.info("END runId={} entityName={} processedCount={}", runId, entity.name(), pending.size());
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


