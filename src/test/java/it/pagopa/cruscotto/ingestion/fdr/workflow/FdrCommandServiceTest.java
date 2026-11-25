package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Il polling delle richieste amministrative, e la parte distruttiva che ne dipende.
 *
 * <p>Qui vivono le garanzie che costano di piu' se saltano, perche' riguardano la cancellazione di
 * dati gia' acquisiti: che il perimetro di una reingestione sia quello chiesto e non un intervallo
 * dedotto, che una richiesta malformata non arrivi mai al {@code TRUNCATE}, e che il preflight
 * stia <strong>prima</strong> e non dopo.
 */
class FdrCommandServiceTest {

    private static final LocalDate D1 = LocalDate.of(2026, 10, 10);
    private static final LocalDate D2 = LocalDate.of(2026, 11, 15);
    private static final LocalDate D3 = LocalDate.of(2026, 12, 10);

    private final FdrBatchRepository batchRepository = mock(FdrBatchRepository.class);
    private final FdrDayProcessor dayProcessor = mock(FdrDayProcessor.class);
    private final FdrReingestionPreparer preparer = mock(FdrReingestionPreparer.class);
    private final FdrIngestionEventRepository eventRepository =
            mock(FdrIngestionEventRepository.class);
    private final FdrStaleProcessingDetector staleDetector = mock(FdrStaleProcessingDetector.class);
    private final FdrIngestionStatusRepository statusRepository =
            mock(FdrIngestionStatusRepository.class);

    private static final Instant NOW = Instant.parse("2026-11-18T10:00:00Z");

    private final FdrCommandService service = new FdrCommandService(batchRepository, dayProcessor,
            preparer, eventRepository, staleDetector, statusRepository,
            new FdrRetryPolicy(new FdrProperties()), new FdrProperties(),
            Clock.fixed(NOW, ZoneOffset.UTC));

    // ─── Polling e claim ─────────────────────────────────────────────────────────

    @Test
    void senzaRichiesteNonFaNulla() {
        when(batchRepository.findPendingCommands()).thenReturn(List.of());

        assertThat(service.pollAndRun()).isZero();
        verify(dayProcessor, never()).process(anyLong(), any());
    }

    /**
     * Il claim perso non e' un errore: e' un altro pod che e' arrivato prima.
     *
     * <p>La richiesta non va ne' riprovata ne' segnalata — sta gia' girando altrove. L'unica cosa
     * che non deve succedere e' elaborarla lo stesso, che e' il motivo per cui il claim e'
     * un'{@code UPDATE} condizionata e non una lettura seguita da una scrittura.
     */
    @Test
    void unaRichiestaGiaPresaDaUnAltroPodVienePassataOltre() {
        when(batchRepository.findPendingCommands()).thenReturn(List.of(bootstrap(1L, D1, D1)));
        when(batchRepository.claim(eq(1L), any())).thenReturn(false);

        assertThat(service.pollAndRun()).isZero();
        verify(dayProcessor, never()).process(anyLong(), any());
        verify(eventRepository, never()).record(anyLong(), any(), any(), any(), any(), anyString(),
                anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    void piuRichiesteVengonoEseguiteNellOrdineInCuiArrivano() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(bootstrap(1L, D1, D1), bootstrap(2L, D3, D3)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        assertThat(service.pollAndRun()).isEqualTo(2);

        InOrder order = inOrder(dayProcessor);
        order.verify(dayProcessor).process(1L, D1);
        order.verify(dayProcessor).process(2L, D3);
    }

    // ─── BOOTSTRAP ───────────────────────────────────────────────────────────────

    /**
     * L'intervallo lo espande l'Ingestor, dopo il claim.
     *
     * <p>E' l'asimmetria voluta rispetto alla REINGESTION: per il BOOTSTRAP la forma della richiesta
     * <em>e'</em> un intervallo, e la granularita' per dataset resta un dettaglio interno nostro —
     * se la conoscesse il backend, cambiarla domani sarebbe un cambio di contratto.
     */
    @Test
    void ilBootstrapEspandeLIntervalloInGiornateConsecutive() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(bootstrap(1L, LocalDate.of(2026, 9, 14),
                        LocalDate.of(2026, 9, 16))));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        InOrder order = inOrder(dayProcessor);
        order.verify(dayProcessor).process(1L, LocalDate.of(2026, 9, 14));
        order.verify(dayProcessor).process(1L, LocalDate.of(2026, 9, 15));
        order.verify(dayProcessor).process(1L, LocalDate.of(2026, 9, 16));
        verify(dayProcessor, times(3)).process(anyLong(), any());
        // Due status per giornata: la giornata e' l'unita', i dataset sono inseparabili.
        verify(batchRepository, times(6)).createStatusIfAbsent(eq(1L), any(), any(), any());
    }

    @Test
    void unBootstrapSenzaIntervalloVieneRespinto() {
        when(batchRepository.findPendingCommands()).thenReturn(List.of(bootstrap(1L, null, null)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);

        service.pollAndRun();

        verify(dayProcessor, never()).process(anyLong(), any());
        verify(batchRepository).complete(eq(1L), eq(FdrProcessingStatus.FAILED), any());
        assertThat(recordedCodes()).contains("EMPTY_REQUEST");
    }

    // ─── REINGESTION sparsa ──────────────────────────────────────────────────────

    /**
     * Il perimetro viene dagli status, e la differenza e' l'intero punto del modello.
     *
     * <p>10/10, 15/11 e 10/12 sono tre giornate. Lette come intervallo sarebbero <em>sessantadue</em>,
     * e sessantanove giornate che nessuno ha chiesto verrebbero svuotate e riscritte.
     */
    @Test
    void unaReingestionSparsaElaboraEsattamenteLeGiornateChieste() {
        reingestionOf(1L, List.of(D1, D2, D3));
        when(preparer.preflight(any())).thenReturn(rebuildable());
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        verify(dayProcessor, times(3)).process(anyLong(), any());
        InOrder order = inOrder(dayProcessor);
        order.verify(dayProcessor).process(1L, D1);
        order.verify(dayProcessor).process(1L, D2);
        order.verify(dayProcessor).process(1L, D3);
        // Nessuna delle sessanta giornate intermedie viene toccata.
        verify(dayProcessor, never()).process(1L, LocalDate.of(2026, 10, 11));
        verify(preparer, never()).guardAndTruncate(LocalDate.of(2026, 11, 1));
    }

    /**
     * Gli estremi della richiesta, se valorizzati, non sono il perimetro.
     *
     * <p>Il backend potrebbe un giorno riempirli come riassunto (minimo e massimo). Leggerli come
     * perimetro trasformerebbe quel riassunto in un ordine di cancellazione.
     */
    @Test
    void gliEstremiDellaRichiestaNonSostituisconoGliStatus() {
        FdrBatchRepository.FdrBatchRow batch =
                new FdrBatchRepository.FdrBatchRow(1L, FdrWorkflow.REINGESTION, D1, D3);
        when(batchRepository.findPendingCommands()).thenReturn(List.of(batch));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(batchRepository.findBusinessDays(1L)).thenReturn(List.of(D1, D3));
        when(batchRepository.findDatasets(eq(1L), any())).thenReturn(bothDatasets());
        when(preparer.preflight(any())).thenReturn(rebuildable());
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        verify(dayProcessor, times(2)).process(anyLong(), any());
        verify(dayProcessor).process(1L, D1);
        verify(dayProcessor).process(1L, D3);
    }

    // ─── Validazione della richiesta ─────────────────────────────────────────────

    /**
     * Una giornata con un dataset solo e' una richiesta malformata, non una richiesta parziale.
     *
     * <p>E va respinta <strong>prima</strong> di qualunque operazione distruttiva: respingerla a
     * meta' significherebbe una giornata con i FLUSSI svuotati e gli IUV vecchi, che e' lo stato
     * peggiore possibile perche' sembra popolata.
     */
    @Test
    void unaReingestionConUnSoloDatasetNonTroncaNulla() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(new FdrBatchRepository.FdrBatchRow(1L, FdrWorkflow.REINGESTION,
                        null, null)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(batchRepository.findBusinessDays(1L)).thenReturn(List.of(D1));
        when(batchRepository.findDatasets(1L, D1))
                .thenReturn(List.of(FdrDataset.FLUSSI_RENDICONTAZIONE.name()));

        service.pollAndRun();

        verify(preparer, never()).preflight(any());
        verify(preparer, never()).guardAndTruncate(any());
        verify(dayProcessor, never()).process(anyLong(), any());
        verify(batchRepository).complete(eq(1L), eq(FdrProcessingStatus.FAILED), any());
        assertThat(recordedCodes()).contains("INVALID_REINGESTION_REQUEST");
    }

    /**
     * Una giornata invalida invalida l'intera richiesta, non solo se stessa.
     *
     * <p>Diversamente da un preflight fallito — che e' un fatto sulla sorgente e riguarda quella
     * giornata — una richiesta malformata dice che chi l'ha scritta non ha capito il contratto:
     * eseguire le altre giornate significherebbe indovinare cosa voleva.
     */
    @Test
    void unaGiornataMalformataFermaTuttaLaRichiesta() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(new FdrBatchRepository.FdrBatchRow(1L, FdrWorkflow.REINGESTION,
                        null, null)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(batchRepository.findBusinessDays(1L)).thenReturn(List.of(D1, D2));
        when(batchRepository.findDatasets(1L, D1)).thenReturn(bothDatasets());
        when(batchRepository.findDatasets(1L, D2))
                .thenReturn(List.of(FdrDataset.IUV_RENDICONTATI.name()));

        service.pollAndRun();

        verify(dayProcessor, never()).process(anyLong(), any());
        verify(preparer, never()).guardAndTruncate(any());
    }

    /** Due righe per lo stesso dataset non sono "entrambi i dataset": il conteggio non basta. */
    @Test
    void unDatasetDuplicatoNonVieneScambiatoPerLaCoppiaCompleta() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(new FdrBatchRepository.FdrBatchRow(1L, FdrWorkflow.REINGESTION,
                        null, null)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(batchRepository.findBusinessDays(1L)).thenReturn(List.of(D1));
        when(batchRepository.findDatasets(1L, D1))
                .thenReturn(List.of(FdrDataset.FLUSSI_RENDICONTAZIONE.name(),
                        FdrDataset.FLUSSI_RENDICONTAZIONE.name()));

        service.pollAndRun();

        verify(preparer, never()).guardAndTruncate(any());
        verify(dayProcessor, never()).process(anyLong(), any());
    }

    // ─── Preflight e ordine delle operazioni ─────────────────────────────────────

    /** L'ordine e' la garanzia: prima si verifica che si possa ricostruire, poi si svuota. */
    @Test
    void ilPreflightPrecedeIlTruncateCheprecedeLElaborazione() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(D1)).thenReturn(rebuildable());
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        InOrder order = inOrder(preparer, dayProcessor);
        order.verify(preparer).preflight(D1);
        order.verify(preparer).guardAndTruncate(D1);
        order.verify(dayProcessor).process(1L, D1);
    }

    @Test
    void unaGiornataNonRicostruibileNonVieneSvuotata() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(D1)).thenReturn(notRebuildable("DATASET_MISSING"));

        service.pollAndRun();

        verify(preparer, never()).guardAndTruncate(any());
        verify(dayProcessor, never()).process(anyLong(), any());
        assertThat(recordedCodes()).contains("DATASET_MISSING");
    }

    /**
     * Le giornate sparse sono indipendenti, e il fatto che una non sia ricostruibile non e' una
     * ragione per lasciare le altre come stanno.
     */
    @Test
    void unaGiornataNonRicostruibileNonFermaLeAltre() {
        reingestionOf(1L, List.of(D1, D2, D3));
        when(preparer.preflight(D1)).thenReturn(rebuildable());
        when(preparer.preflight(D2)).thenReturn(notRebuildable("EMPTY_DATASET"));
        when(preparer.preflight(D3)).thenReturn(rebuildable());
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        verify(dayProcessor).process(1L, D1);
        verify(dayProcessor).process(1L, D3);
        verify(dayProcessor, never()).process(1L, D2);
        verify(preparer, never()).guardAndTruncate(D2);
    }

    /**
     * Un guasto tecnico non e' un "non ricostruibile": e' un "non lo so".
     *
     * <p>Azure irraggiungibile non dice nulla sui file, quindi la giornata resta intatta e l'evento
     * e' di classe tecnica — quella che il core sa riprendere. Trattarlo come un preflight fallito
     * lo renderebbe terminale, e una giornata recuperabile verrebbe abbandonata per un timeout.
     */
    @Test
    void unGuastoTecnicoNelPreflightLasciaLaGiornataIntatta() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(D1)).thenThrow(new IllegalStateException("azure unreachable"));

        service.pollAndRun();

        verify(preparer, never()).guardAndTruncate(any());
        verify(dayProcessor, never()).process(anyLong(), any());
        assertThat(recordedTypes()).contains(FdrEventType.TECHNICAL_ERROR);
    }

    /**
     * Una giornata rifiutata non resta {@code PENDING}: il suo esito va scritto.
     *
     * <p>Senza, la giornata sembrerebbe ancora da fare per sempre e — peggio — l'esito del batch, che
     * si compone dagli status, non terrebbe conto del rifiuto: un batch con una giornata respinta
     * risulterebbe completato.
     */
    @Test
    void unaGiornataRifiutataRiceveIlSuoEsitoENonRestaInAttesa() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(D1)).thenReturn(notRebuildable("EMPTY_DATASET"));

        service.pollAndRun();

        verify(statusRepository).settleDay(eq(1L), eq(D1), eq(FdrProcessingStatus.PENDING),
                eq(FdrProcessingStatus.FAILED), eq("EMPTY_DATASET"), anyString(), isNull(), any());
    }

    /**
     * Un guasto tecnico lascia invece la giornata ritentabile, con un orario per il tentativo.
     *
     * <p>La differenza rispetto al caso sopra e' tutta qui: della sorgente non si sa ancora nulla,
     * quindi chiuderla come fallita butterebbe via una giornata che bastava rileggere.
     */
    @Test
    void unGuastoTecnicoLasciaLaGiornataRitentabileConUnOrario() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(D1)).thenThrow(new IllegalStateException("azure unreachable"));

        service.pollAndRun();

        verify(statusRepository).settleDay(eq(1L), eq(D1), eq(FdrProcessingStatus.PENDING),
                eq(FdrProcessingStatus.WAITING_RETRY), eq("IllegalStateException"), anyString(),
                eq(NOW.plus(new FdrProperties().getRetry().getInterval())), any());
    }

    /** Un rifiuto riguarda la richiesta, non una giornata: l'evento non ne inventa una. */
    @Test
    void ilRifiutoDiUnaRichiestaNonSiAttribuisceAUnaGiornata() {
        when(batchRepository.findPendingCommands()).thenReturn(List.of(bootstrap(1L, null, null)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);

        service.pollAndRun();

        verify(eventRepository).record(eq(1L), isNull(), isNull(), isNull(), isNull(), anyString(),
                anyString(), eq("EMPTY_REQUEST"), anyString(), anyString(), any());
    }

    // ─── Recupero delle richieste abbandonate ────────────────────────────────────

    @Test
    void unaRichiestaAbbandonataTornaInCoda() {
        when(batchRepository.findStaleProcessing(any()))
                .thenReturn(List.of(bootstrap(5L, D1, D1)));
        when(staleDetector.isAnyFdrExecutionAlive(any())).thenReturn(false);
        when(batchRepository.releaseToPending(eq(5L), any())).thenReturn(true);
        when(batchRepository.findPendingCommands()).thenReturn(List.of());

        service.pollAndRun();

        verify(batchRepository).releaseToPending(eq(5L), any());
        assertThat(recordedCodes()).contains("PROCESSING_ABANDONED");
    }

    /**
     * Finche' un'esecuzione FDR batte, nulla e' abbandonato.
     *
     * <p>E' la differenza fra un recupero e un raddoppio: rimettere in coda una richiesta che sta
     * ancora girando produrrebbe due worker sulla stessa giornata, e per una REINGESTION due
     * {@code TRUNCATE} intrecciati alle rispettive riscritture.
     */
    @Test
    void finchePulsaUnEsecuzioneFdrNessunaRichiestaVieneRecuperata() {
        when(batchRepository.findStaleProcessing(any()))
                .thenReturn(List.of(bootstrap(5L, D1, D1)));
        when(staleDetector.isAnyFdrExecutionAlive(any())).thenReturn(true);
        when(batchRepository.findPendingCommands()).thenReturn(List.of());

        service.pollAndRun();

        verify(batchRepository, never()).releaseToPending(anyLong(), any());
    }

    // ─── Esito del batch ─────────────────────────────────────────────────────────

    @Test
    void lEsitoDelBatchVieneDagliStatusDelleSueGiornate() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(any())).thenReturn(rebuildable());
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));
        when(batchRepository.findStatuses(1L)).thenReturn(List.of(FdrProcessingStatus.COMPLETED,
                FdrProcessingStatus.COMPLETED_WITH_WARNING));

        service.pollAndRun();

        verify(batchRepository).complete(eq(1L), eq(FdrProcessingStatus.COMPLETED_WITH_WARNING),
                any());
    }

    // ─── Utilita' ────────────────────────────────────────────────────────────────

    private void reingestionOf(long batchId, List<LocalDate> days) {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(new FdrBatchRepository.FdrBatchRow(batchId,
                        FdrWorkflow.REINGESTION, null, null)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(batchRepository.findBusinessDays(batchId)).thenReturn(days);
        when(batchRepository.findDatasets(eq(batchId), any())).thenReturn(bothDatasets());
    }

    private static List<String> bothDatasets() {
        return List.of(FdrDataset.FLUSSI_RENDICONTAZIONE.name(), FdrDataset.IUV_RENDICONTATI.name());
    }

    private static FdrBatchRepository.FdrBatchRow bootstrap(long id, LocalDate from, LocalDate to) {
        return new FdrBatchRepository.FdrBatchRow(id, FdrWorkflow.BOOTSTRAP, from, to);
    }

    private static FdrReingestionPreparer.FdrPreflightResult rebuildable() {
        return new FdrReingestionPreparer.FdrPreflightResult(true, null, null, null);
    }

    private static FdrReingestionPreparer.FdrPreflightResult notRebuildable(String code) {
        return new FdrReingestionPreparer.FdrPreflightResult(false,
                FdrDataset.IUV_RENDICONTATI, code, "not rebuildable");
    }

    private static FdrDayResult result(FdrProcessingStatus status) {
        Map<FdrDataset, FdrDatasetResult> datasets = new EnumMap<>(FdrDataset.class);
        return new FdrDayResult(1L, D1, status, datasets, false);
    }

    private List<String> recordedCodes() {
        ArgumentCaptor<String> code = ArgumentCaptor.forClass(String.class);
        verify(eventRepository, org.mockito.Mockito.atLeastOnce()).record(anyLong(), any(), any(),
                any(), any(), anyString(), anyString(), code.capture(), anyString(), any(), any());
        return code.getAllValues();
    }

    private List<String> recordedTypes() {
        ArgumentCaptor<String> type = ArgumentCaptor.forClass(String.class);
        verify(eventRepository, org.mockito.Mockito.atLeastOnce()).record(anyLong(), any(), any(),
                any(), any(), type.capture(), anyString(), anyString(), anyString(), any(), any());
        return type.getAllValues();
    }
}
