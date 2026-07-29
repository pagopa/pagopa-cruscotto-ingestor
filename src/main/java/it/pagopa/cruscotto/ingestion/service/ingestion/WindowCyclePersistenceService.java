package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
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
                    log.debug("BULK_WRITE_CHUNK_BEGIN runId={} entity={} chunkSize={} totalSize={}",
                            ctx.getRunId(), entity.name(), chunk.size(), payload.size());
                    BulkWriteResult chunkResult = persistBulkTransactional(entity, chunk, ctx.getRunId(), ctx.getBatchLocalCache());
                    log.debug("BULK_WRITE_CHUNK_OK runId={} entity={} inserted={}",
                            ctx.getRunId(), entity.name(), chunkResult.getRowsInserted());
                    rowsInserted += chunkResult.getRowsInserted();
                    if (chunkResult.getMaxInsertedTimestamp() != null) {
                        maxInsertedTs = chunkResult.getMaxInsertedTimestamp();
                    }
                } catch (Exception ex) {
                    log.error("BULK_WRITE_CHUNK_FAILED runId={} entity={} error={}",
                            ctx.getRunId(), entity.name(), ex.getMessage(), ex);
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

        // STEP 3: Update checkpoint only when at least one row has been ingested.
        if (rowsInserted > 0) {
            try {
                log.debug("CHECKPOINT_UPDATE_BEGIN runId={} entity={} checkpointTs={}", ctx.getRunId(), entity.name(), checkpointTs);
                updateCheckpointTransactional(entity, checkpointTs, ctx.getRunId());
                log.info("PERSIST_CYCLE_OK runId={} entity={} checkpointTs={} rowsInserted={} stagedCount={} discardedCount={}",
                        ctx.getRunId(), entity.name(), checkpointTs, rowsInserted, stagedCount, discardedCount);
            } catch (Exception ex) {
                log.error("CHECKPOINT_UPDATE_FAILED runId={} entity={} error={}",
                        ctx.getRunId(), entity.name(), ex.getMessage(), ex);
                throw new RuntimeException("Failed to update checkpoint: " + ex.getMessage(), ex);
            }
        } else {
            log.info("PERSIST_CYCLE_OK_NO_CHECKPOINT runId={} entity={} rowsInserted={} stagedCount={} discardedCount={}",
                    ctx.getRunId(), entity.name(), rowsInserted, stagedCount, discardedCount);
        }
        return new WindowCycleResult(rowsInserted, stagedCount, maxInsertedTs);
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
            log.debug("STAGING_PERSIST_OK runId={} entity={} count={}", ctx.getRunId(), entity.name(), count);
            return count;
        } catch (Exception ex) {
            log.error("STAGING_PERSIST_FAILED runId={} entity={} error={}", ctx.getRunId(), entity.name(), ex.getMessage(), ex);
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
            log.debug("DISCARDED_PERSIST_OK runId={} entity={} count={}", ctx.getRunId(), entity.name(), count);
            return count;
        } catch (Exception ex) {
            log.error("DISCARDED_PERSIST_FAILED runId={} entity={} error={}", ctx.getRunId(), entity.name(), ex.getMessage(), ex);
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
