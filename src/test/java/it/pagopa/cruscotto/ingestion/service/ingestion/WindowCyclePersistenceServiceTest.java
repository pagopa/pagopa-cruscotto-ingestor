package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.CheckpointStoreService;
import it.pagopa.cruscotto.ingestion.service.PositionEventUpdateService;
import it.pagopa.cruscotto.ingestion.service.StagingErrorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Verifies the checkpoint-advance policy: a fully-processed window advances the checkpoint even
 * when nothing was inserted (all rows staged/discarded), so the window is not re-read from ADX;
 * a window that produced no outcome at all leaves the checkpoint untouched.
 */
@ExtendWith(MockitoExtension.class)
class WindowCyclePersistenceServiceTest {

    @Mock
    private BulkWriter bulkWriter;
    @Mock
    private CheckpointStoreService checkpointStore;
    @Mock
    private StagingErrorService stagingErrorService;
    @Mock
    private PositionEventUpdateService positionEventUpdateService;

    private WindowCyclePersistenceService service;

    @BeforeEach
    void setUp() {
        service = new WindowCyclePersistenceService(
                bulkWriter, checkpointStore, stagingErrorService, new IngestionConfig(), positionEventUpdateService);
    }

    private RunContext ctx() {
        return new RunContext(EntityName.POSITION_TOKENS.name(), "run-1", Instant.now());
    }

    @Test
    void allStagedWindowAdvancesCheckpoint() throws Exception {
        Instant checkpointTs = Instant.parse("2026-08-05T10:00:00Z");
        List<WindowCyclePersistenceService.StagingRecord> staging = List.of(
                new WindowCyclePersistenceService.StagingRecord("key-1", Map.of("NAV", "NAV-1"),
                        new MissingForeignKeyException("Missing required FK fkPosition")));
        when(stagingErrorService.insertErrorsBulk(any(RunContext.class), any())).thenReturn(1L);

        service.persistWindowCycle(ctx(), EntityName.POSITION_TOKENS, List.of(), staging, 0L, checkpointTs);

        // 0 rows inserted but the window was fully processed (all staged) -> checkpoint must advance.
        verify(checkpointStore).updateCheckpoint(EntityName.POSITION_TOKENS, checkpointTs, "run-1");
    }

    /**
     * Una riga scartata di proposito non finisce in {@code STG_INGEST_ERROR}.
     *
     * <p>Decisione del cliente del 2026-11-26: una EXTRA_INFO fuori whitelist va scartata, non
     * archiviata. Finivano nello staging con {@code STATUS = DONE} e il payload intero — righe
     * terminali che nessuna riconciliazione avrebbe mai ripreso — al ritmo di oltre due milioni al
     * giorno su dev, fino a riempire lo storage e mandare l'istanza in sola lettura.
     *
     * <p>Il test guarda il collaboratore e non il database: se qualcuno reintroducesse una
     * scrittura, qui si vedrebbe subito, mentre a valle si vedrebbe fra settimane sotto forma di
     * disco pieno.
     */
    @Test
    void discardedRowsAreNeverWrittenToStaging() throws Exception {
        Instant checkpointTs = Instant.parse("2026-08-05T10:10:00Z");
        service.persistWindowCycle(ctx(), EntityName.EXTRA_INFO, List.of(), List.of(), 2L, checkpointTs);

        verify(stagingErrorService, never()).insertErrorsBulk(any(RunContext.class), any());
        verifyNoMoreInteractions(stagingErrorService);
    }

    /**
     * ...e la finestra avanza comunque il checkpoint. È la metà della modifica che si dimentica.
     *
     * <p>Il conteggio degli scarti è una delle tre condizioni che fanno avanzare il checkpoint.
     * Smettendo di scriverli, la tentazione è di prendere quel numero dall'esito della scrittura —
     * che ora è zero — e il risultato sarebbe una finestra fatta <strong>interamente</strong> di
     * scarti che non avanza mai: riletta da ADX a ogni run, per sempre. Su EXTRA_INFO, con una
     * whitelist volutamente stretta, quella finestra è il caso normale e non un'ipotesi.
     */
    @Test
    void allDiscardedWindowStillAdvancesCheckpoint() throws Exception {
        Instant checkpointTs = Instant.parse("2026-08-05T10:15:00Z");
        service.persistWindowCycle(ctx(), EntityName.EXTRA_INFO, List.of(), List.of(), 1L, checkpointTs);

        verify(checkpointStore).updateCheckpoint(EntityName.EXTRA_INFO, checkpointTs, "run-1");
    }

    @Test
    void insertedWindowAdvancesCheckpoint() throws Exception {
        Instant checkpointTs = Instant.parse("2026-08-05T10:05:00Z");
        when(bulkWriter.writeBulk(eq(EntityName.POSITION_TOKENS), any(), eq("run-1"), any()))
                .thenReturn(new BulkWriteResult(1, checkpointTs));

        service.persistWindowCycle(ctx(), EntityName.POSITION_TOKENS,
                List.of(new PositionTokens()), List.of(), 0L, checkpointTs);

        verify(checkpointStore).updateCheckpoint(EntityName.POSITION_TOKENS, checkpointTs, "run-1");
    }

    @Test
    void emptyOutcomeWindowLeavesCheckpointUntouched() throws Exception {
        Instant checkpointTs = Instant.parse("2026-08-05T10:10:00Z");

        service.persistWindowCycle(ctx(), EntityName.POSITION_TOKENS, List.of(), List.of(), 0L, checkpointTs);

        // Nothing inserted, staged or discarded -> no checkpoint write.
        verify(checkpointStore, never()).updateCheckpoint(any(), any(), any());
    }
}
