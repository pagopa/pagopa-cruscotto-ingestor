package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.CheckpointStoreService;
import it.pagopa.cruscotto.ingestion.service.PositionEventUpdateService;
import it.pagopa.cruscotto.ingestion.service.StagingErrorService;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class WindowCyclePersistenceService {

    private final BulkWriter bulkWriter;
    private final CheckpointStoreService checkpointStore;
    private final StagingErrorService stagingErrorService;
    private final IngestionConfig ingestionConfig;
    private final PositionEventUpdateService positionEventUpdateService;

    /**
     * Persists one ADX window in a dedicated transaction.
     * The batch tasklet has its own transaction, so this REQUIRES_NEW boundary
     * ensures the data write and checkpoint update are committed even if the
     * surrounding step later fails.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public WindowCycleResult persistWindowCycle(
            RunContext ctx,
            EntityName entity,
            List<Object> payload,
            List<StagingRecord> stagingRecords,
            List<DiscardedRecord> discardedRecords,
            Instant checkpointTs)
            throws BulkWriter.BulkWriteException {

        // STEP 1: Persist staging errors in independent transaction
        long stagedCount = persistStagingErrorsTransactional(ctx, entity, stagingRecords);
        long discardedCount = persistDiscardedRecordsTransactional(ctx, entity, discardedRecords);

        // STEP 2: Persist bulk data in independent transaction
        int rowsInserted = 0;
        Instant maxInsertedTs = checkpointTs;
        if (payload != null && !payload.isEmpty()) {
            int bulkSize = Math.max(1, ingestionConfig.getBulkInsertSize());
            for (int i = 0; i < payload.size(); i += bulkSize) {
                int end = Math.min(i + bulkSize, payload.size());
                List<Object> chunk = payload.subList(i, end);
                try {
                    BulkWriteResult chunkResult = persistBulkTransactional(entity, chunk, ctx.getRunId(), ctx.getBatchLocalCache());
                    // Una riga sola per chunk, non due: la coppia BEGIN/OK raddoppiava il volume
                    // senza aggiungere nulla che l'esito non dicesse gia'.
                    try (LogDetails ignored = LogDetails.open()
                            .with(LogFields.DEPENDENCY, "postgres")
                            .with("chunk_size", chunk.size())
                            .with("payload_size", payload.size())
                            .with(LogFields.ROWS_INSERTED, chunkResult.getRowsInserted())) {
                        LogHelper.debug(ctx, RunPhase.BULK_OK, "Wrote payload chunk");
                    }
                    rowsInserted += chunkResult.getRowsInserted();
                    if (chunkResult.getMaxInsertedTimestamp() != null) {
                        maxInsertedTs = chunkResult.getMaxInsertedTimestamp();
                    }
                } catch (Exception ex) {
                    try (LogDetails ignored = LogDetails.open()
                            .with(LogFields.DEPENDENCY, "postgres")
                            .with(LogFields.ERROR_DETAIL, ex.getMessage())) {
                        LogHelper.error(ctx, RunPhase.BULK_KO_TOTAL, "Failed to write payload chunk", ex);
                    }
                    throw new BulkWriter.BulkWriteException("Chunk bulk write failed: " + ex.getMessage(), ex);
                }
            }

            if (entity == EntityName.EVENTS_WF) {
                List<EventsWf> insertedEvents = payload.stream()
                        .filter(EventsWf.class::isInstance)
                        .map(EventsWf.class::cast)
                        .toList();
                if (!insertedEvents.isEmpty()) {
                    positionEventUpdateService.updatePositionAfterEvents(ctx, insertedEvents);
                }
            }
        }

        // STEP 3: Advance the checkpoint when the window was fully processed, i.e. every ADX row was
        // accounted for as inserted, staged or discarded. Staged/discarded rows are already durably
        // committed above (STEP 1, independent transactions), so advancing the checkpoint over an
        // all-staged / all-discarded window loses no data (reconciliation will retry the staged rows)
        // and prevents the window from being re-read from ADX and re-staged on every subsequent run.
        // The checkpointTs is guardrail/deferral-aware (see resolveCheckpointTimestamp), so it never
        // advances past rows that were not processed.
        if (rowsInserted > 0 || stagedCount > 0 || discardedCount > 0) {
            try {
                updateCheckpointTransactional(entity, checkpointTs, ctx.getRunId());
                // PERSIST_CYCLE_OK rimosso: GenericIngestionRunnerImpl emette gia' "Persisted
                // operation cycle" con gli stessi campi (righe inserite, righe in staging,
                // checkpoint). Erano due righe per lo stesso fatto, a una chiamata di distanza.
                try (LogDetails ignored = LogDetails.open()
                        .with(LogFields.DEPENDENCY, "postgres")
                        .with(LogFields.CHECKPOINT_TS, checkpointTs)) {
                    LogHelper.debug(ctx, RunPhase.CHECKPOINT, "Updated checkpoint");
                }
            } catch (Exception ex) {
                try (LogDetails ignored = LogDetails.open()
                        .with(LogFields.DEPENDENCY, "postgres")
                        .with(LogFields.CHECKPOINT_TS, checkpointTs)
                        .with(LogFields.ERROR_DETAIL, ex.getMessage())) {
                    LogHelper.error(ctx, RunPhase.ERROR, "Failed to update checkpoint", ex);
                }
                throw new RuntimeException("Failed to update checkpoint: " + ex.getMessage(), ex);
            }
        }
        // Il ramo senza checkpoint non logga: il ciclo e' comunque riepilogato dal chiamante.
        return new WindowCycleResult(rowsInserted, stagedCount, maxInsertedTs);
    }

    private static void logPersisted(RunContext ctx, RunPhase phase, String message, long count) {
        try (LogDetails ignored = LogDetails.open()
                .with(LogFields.DEPENDENCY, "postgres")
                .with(LogFields.ROWS, count)) {
            LogHelper.debug(ctx, phase, message);
        }
    }

    private static void logPersistFailed(RunContext ctx, String message, Exception ex) {
        try (LogDetails ignored = LogDetails.open()
                .with(LogFields.DEPENDENCY, "postgres")
                .with(LogFields.ERROR_DETAIL, ex.getMessage())) {
            LogHelper.error(ctx, RunPhase.ERROR, message, ex);
        }
    }

    private long persistStagingErrorsTransactional(RunContext ctx, EntityName entity, List<StagingRecord> stagingRecords) {
        if (stagingRecords == null || stagingRecords.isEmpty()) {
            return 0;
        }
        try {
            List<StagingErrorService.StagingInputRecord> batch = stagingRecords.stream()
                    .map(record -> new StagingErrorService.StagingInputRecord(
                            record.sourceKey(),
                            record.payload(),
                            record.exception()))
                    .toList();
            long count = stagingErrorService.insertErrorsBulk(ctx, batch);
            logPersisted(ctx, RunPhase.BULK_OK, "Persisted staging errors", count);
            return count;
        } catch (Exception ex) {
            logPersistFailed(ctx, "Failed to persist staging errors", ex);
            throw new RuntimeException("Failed to persist staging errors: " + ex.getMessage(), ex);
        }
    }

    private long persistDiscardedRecordsTransactional(RunContext ctx, EntityName entity, List<DiscardedRecord> discardedRecords) {
        if (discardedRecords == null || discardedRecords.isEmpty()) {
            return 0;
        }
        try {
            List<StagingErrorService.DiscardedInputRecord> batch = discardedRecords.stream()
                    .map(record -> new StagingErrorService.DiscardedInputRecord(
                            record.sourceKey(),
                            record.payload(),
                            record.reason()))
                    .toList();
            long count = stagingErrorService.insertDiscardedBulk(ctx, batch);
            logPersisted(ctx, RunPhase.BULK_OK, "Persisted discarded records", count);
            return count;
        } catch (Exception ex) {
            logPersistFailed(ctx, "Failed to persist discarded records", ex);
            throw new RuntimeException("Failed to persist discarded records: " + ex.getMessage(), ex);
        }
    }

    private BulkWriteResult persistBulkTransactional(EntityName entity, List<Object> chunk, String runId, BatchLocalCache batchCache)
            throws BulkWriter.BulkWriteException {
        return bulkWriter.writeBulk(entity, chunk, runId, batchCache);
    }

    private void updateCheckpointTransactional(EntityName entity, Instant checkpointTs, String runId) {
        checkpointStore.updateCheckpoint(entity, checkpointTs, runId);
    }

    public record StagingRecord(String sourceKey, Map<String, Object> payload, Exception exception) {
    }

    public record DiscardedRecord(String sourceKey, Map<String, Object> payload, String reason) {
    }

    public record WindowCycleResult(long rowsInserted, long rowsStaged, Instant maxInsertedTimestamp) {
    }
}
