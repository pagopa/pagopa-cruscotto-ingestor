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
            long discardedCount,
            Instant checkpointTs)
            throws BulkWriter.BulkWriteException {

        // STEP 1: Persist staging errors in independent transaction
        long stagedCount = persistStagingErrorsTransactional(ctx, entity, stagingRecords);

        // GLI SCARTI NON SI PERSISTONO PIU'. Decisione del cliente del 2026-11-26: una EXTRA_INFO
        // fuori whitelist va scartata, non archiviata. Finivano in STG_INGEST_ERROR con STATUS=DONE
        // e il payload completo, cioe' ~1,1 KB per riga: su dev oltre due milioni di righe in
        // quattro ore, circa 2,3 GB al giorno di scarti deliberati, fino a riempire lo storage e
        // mandare l'istanza in sola lettura. Sono righe terminali per costruzione — DONE, mai
        // riconciliate da nessuno — quindi conservarle aveva valore solo finche' si metteva a punto
        // la whitelist.
        //
        // IL CONTEGGIO ARRIVA DAL CHIAMANTE, e non e' un dettaglio: e' una delle tre condizioni
        // che fanno avanzare il checkpoint allo STEP 3. Ricavarlo dall'esito di una scrittura che
        // non avviene piu' - cioe' zero - avrebbe bloccato per sempre ogni finestra composta
        // interamente da scarti, che su EXTRA_INFO, con una whitelist volutamente stretta, e' il
        // caso normale e non un'ipotesi: la finestra sarebbe stata riletta da ADX a ogni run.

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

        // STEP 3: si avanza il checkpoint quando la finestra e' stata lavorata per intero, cioe'
        // quando ogni riga letta da ADX risulta inserita, messa in staging o scartata.
        //
        // Le tre voci non hanno pero' la stessa natura, e la differenza e' il motivo per cui
        // avanzare e' comunque sicuro:
        //   - inserite:  sono in tabella;
        //   - in staging: committate allo STEP 1 in transazione propria, e la riconciliazione le
        //                 riprendera';
        //   - SCARTATE:  non esistono piu' da nessuna parte, ed e' voluto. Non c'e' niente da
        //                 recuperare perche' non c'e' niente che si voglia recuperare: sono righe
        //                 che una regola di business dice di buttare.
        //
        // In tutti e tre i casi la riga e' stata DECISA, ed e' questo che conta: senza avanzare, la
        // finestra verrebbe riletta da ADX a ogni run per sempre.
        //
        // checkpointTs tiene gia' conto di guardrail e differimenti (vedi resolveCheckpointTimestamp),
        // quindi non supera mai righe che non sono state lavorate.
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

    private BulkWriteResult persistBulkTransactional(EntityName entity, List<Object> chunk, String runId, BatchLocalCache batchCache)
            throws BulkWriter.BulkWriteException {
        return bulkWriter.writeBulk(entity, chunk, runId, batchCache);
    }

    private void updateCheckpointTransactional(EntityName entity, Instant checkpointTs, String runId) {
        checkpointStore.updateCheckpoint(entity, checkpointTs, runId);
    }

    public record StagingRecord(String sourceKey, Map<String, Object> payload, Exception exception) {
    }


    public record WindowCycleResult(long rowsInserted, long rowsStaged, Instant maxInsertedTimestamp) {
    }
}
