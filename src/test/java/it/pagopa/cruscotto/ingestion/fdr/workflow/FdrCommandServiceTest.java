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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doReturn;
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
    private final FdrDayOwnership ownership = mock(FdrDayOwnership.class);

    /**
     * Il possesso della giornata e' concesso per default.
     *
     * <p>Questi test riguardano l'<em>orchestrazione</em> — quali giornate, in che ordine, con
     * quali esiti — non il possesso, che ha i suoi test in {@link FdrDayOwnershipTest}. Senza
     * questo stub il mock risponderebbe "la giornata e' di un altro" e ogni test verificherebbe
     * soltanto che non si fa nulla. I test che vogliono proprio quel caso lo ristubbano.
     */
    @org.junit.jupiter.api.BeforeEach
    void givenDaysAreClaimable() {
                // doAnswer e NON when(...): su un metodo gia' stubbato con un Answer, when(mock.metodo(...))
        // ESEGUE quell'Answer con argomenti nulli prima di registrare il nuovo stub. Qui la
        // giornata e l'istante arriverebbero null e l'Answer esploderebbe - che e' esattamente
        // cio' che succedeva. Vale per ogni ristubbatura di claim in questa classe.
        doAnswer(invocation ->
                java.util.Optional.of(new FdrDayLease(invocation.getArgument(0),
                        invocation.getArgument(1), "token-1", 1,
                        java.util.Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, 1L,
                                FdrDataset.IUV_RENDICONTATI, 2L),
                        java.util.Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE,
                                FdrProcessingStatus.PENDING,
                                FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.PENDING),
                        invocation.<java.time.Instant>getArgument(3).plusSeconds(300)))).when(ownership).claim(anyLong(), any(), any(), any());
    }

    private final FdrIngestionStatusRepository statusRepository =
            mock(FdrIngestionStatusRepository.class);

    private static final Instant NOW = Instant.parse("2026-11-18T10:00:00Z");

    private final FdrCommandService service = new FdrCommandService(batchRepository, dayProcessor,
            preparer, eventRepository, ownership, statusRepository,
            new FdrRetryPolicy(new FdrProperties()), new FdrProperties(),
            Clock.fixed(NOW, ZoneOffset.UTC));

    // ─── Polling e claim ─────────────────────────────────────────────────────────

    @Test
    void senzaRichiesteNonFaNulla() {
        when(batchRepository.findPendingCommands()).thenReturn(List.of());

        assertThat(service.pollAndRun()).isZero();
        verify(dayProcessor, never()).process(any(), any());
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
        verify(dayProcessor, never()).process(any(), any());
        verify(eventRepository, never()).record(anyLong(), any(), any(), any(), any(), anyString(),
                anyString(), anyString(), anyString(), any(), any());
    }

    @Test
    void piuRichiesteVengonoEseguiteNellOrdineInCuiArrivano() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(bootstrap(1L, D1, D1), bootstrap(2L, D3, D3)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        assertThat(service.pollAndRun()).isEqualTo(2);

        InOrder order = inOrder(dayProcessor);
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D1)), any());
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 2L && l.businessDay().equals(D3)), any());
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
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        InOrder order = inOrder(dayProcessor);
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(LocalDate.of(2026, 9, 14))), any());
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(LocalDate.of(2026, 9, 15))), any());
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(LocalDate.of(2026, 9, 16))), any());
        verify(dayProcessor, times(3)).process(any(), any());
        // Due status per giornata: la giornata e' l'unita', i dataset sono inseparabili.
        verify(batchRepository, times(6)).createStatusIfAbsent(eq(1L), any(), any(), any());
    }

    @Test
    void unBootstrapSenzaIntervalloVieneRespinto() {
        when(batchRepository.findPendingCommands()).thenReturn(List.of(bootstrap(1L, null, null)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);

        service.pollAndRun();

        verify(dayProcessor, never()).process(any(), any());
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
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        verify(dayProcessor, times(3)).process(any(), any());
        InOrder order = inOrder(dayProcessor);
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D1)), any());
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D2)), any());
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D3)), any());
        // Nessuna delle sessanta giornate intermedie viene toccata.
        verify(dayProcessor, never()).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(LocalDate.of(2026, 10, 11))), any());
        verify(preparer, never()).guardAndTruncate(eq(LocalDate.of(2026, 11, 1)), any());
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
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        verify(dayProcessor, times(2)).process(any(), any());
        verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D1)), any());
        verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D3)), any());
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
        verify(preparer, never()).guardAndTruncate(any(), any());
        verify(dayProcessor, never()).process(any(), any());
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

        verify(dayProcessor, never()).process(any(), any());
        verify(preparer, never()).guardAndTruncate(any(), any());
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

        verify(preparer, never()).guardAndTruncate(any(), any());
        verify(dayProcessor, never()).process(any(), any());
    }

    // ─── Preflight e ordine delle operazioni ─────────────────────────────────────

    /** L'ordine e' la garanzia: prima si verifica che si possa ricostruire, poi si svuota. */
    @Test
    void ilPreflightPrecedeIlTruncateCheprecedeLElaborazione() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(D1)).thenReturn(rebuildable());
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        InOrder order = inOrder(preparer, dayProcessor);
        order.verify(preparer).preflight(D1);
        order.verify(preparer).guardAndTruncate(eq(D1), any());
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D1)), any());
    }

    @Test
    void unaGiornataNonRicostruibileNonVieneSvuotata() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(D1)).thenReturn(notRebuildable("DATASET_MISSING"));

        service.pollAndRun();

        verify(preparer, never()).guardAndTruncate(any(), any());
        verify(dayProcessor, never()).process(any(), any());
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
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D1)), any());
        verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D3)), any());
        verify(dayProcessor, never()).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(D2)), any());
        verify(preparer, never()).guardAndTruncate(eq(D2), any());
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

        verify(preparer, never()).guardAndTruncate(any(), any());
        verify(dayProcessor, never()).process(any(), any());
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

        // Da PROCESSING e non da PENDING: quando si arriva al preflight il claim ha gia' portato
        // entrambe le righe in lavorazione. Lo stato di partenza nel WHERE e' cio' che impedisce di
        // sovrascrivere l'esito di un worker diverso dal nostro.
        verify(statusRepository).settleDay(eq(1L), eq(D1), eq(FdrProcessingStatus.PROCESSING),
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

        verify(statusRepository).settleDay(eq(1L), eq(D1), eq(FdrProcessingStatus.PROCESSING),
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
        when(ownership.hasLiveLease(anyLong(), any())).thenReturn(false);
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
        when(ownership.hasLiveLease(anyLong(), any())).thenReturn(true);
        when(batchRepository.findPendingCommands()).thenReturn(List.of());

        service.pollAndRun();

        verify(batchRepository, never()).releaseToPending(anyLong(), any());
    }

    // ─── Esito del batch ─────────────────────────────────────────────────────────

    @Test
    void lEsitoDelBatchVieneDagliStatusDelleSueGiornate() {
        reingestionOf(1L, List.of(D1));
        when(preparer.preflight(any())).thenReturn(rebuildable());
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));
        when(batchRepository.findStatuses(1L)).thenReturn(List.of(FdrProcessingStatus.COMPLETED,
                FdrProcessingStatus.COMPLETED_WITH_WARNING));

        service.pollAndRun();

        verify(batchRepository).complete(eq(1L), eq(FdrProcessingStatus.COMPLETED_WITH_WARNING),
                any());
    }

    // ─── Utilita' ────────────────────────────────────────────────────────────────


    // ─── Giornata incoerente ─────────────────────────────────────────────────────

    /**
     * Una giornata le cui due righe non concordano sul tentativo viene <strong>chiusa</strong>,
     * non ritentata.
     *
     * <p>Il claim si rifiuta e annulla la sua transazione — è giusto: gli status restano la
     * fotografia esatta dell'anomalia. Ma lasciarla lì significa che il poller la ritrova al giro
     * successivo, fallisce allo stesso modo e scrive un altro evento, per sempre: non è un guasto
     * transitorio, rileggere la stessa sorgente ritroverebbe le stesse due righe discordi.
     *
     * <p>Quindi la giornata viene chiusa {@code FAILED} con un codice stabile e <strong>senza</strong>
     * orario per il tentativo successivo. Per rifarla serve una REINGESTION, cioè una decisione
     * presa da qualcuno.
     */
    @Test
    void unaGiornataIncoerenteVieneChiusaEnonElaborata() {
        reingestionOf(1L, List.of(D1));
        doThrow(new FdrInconsistentDayException(1L, D1, "FLUSSI_RENDICONTAZIONE=1 IUV_RENDICONTATI=4"))
                .when(ownership).claim(anyLong(), any(), any(), any());

        service.pollAndRun();

        verify(statusRepository).settleInconsistentDay(eq(1L), eq(D1),
                eq(FdrInconsistentDayException.CODE), anyString(), any());
        verify(eventRepository).record(eq(1L), isNull(), eq(D1), isNull(), isNull(),
                eq(FdrEventType.TECHNICAL_ERROR), eq(FdrEventType.ERROR),
                eq(FdrInconsistentDayException.CODE), anyString(), anyString(), any());
        verify(dayProcessor, never()).process(any(), any());
        verify(preparer, never()).guardAndTruncate(any(), any());
    }

    /** Le altre giornate della stessa richiesta non vengono fermate dall'incoerenza di una. */
    @Test
    void unaGiornataIncoerenteNonFermaLeAltre() {
        reingestionOf(1L, List.of(D1, D3));
        when(preparer.preflight(any())).thenReturn(
                new FdrReingestionPreparer.FdrPreflightResult(true, null, null, null));
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));
        doAnswer(invocation -> {
            if (D1.equals(invocation.getArgument(1))) {
                throw new FdrInconsistentDayException(1L, D1, "divergenti");
            }
            return java.util.Optional.of(new FdrDayLease(invocation.getArgument(0),
                    invocation.getArgument(1), "token-1", 1,
                    java.util.Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, 1L,
                            FdrDataset.IUV_RENDICONTATI, 2L),
                    java.util.Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE,
                            FdrProcessingStatus.PENDING,
                            FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.PENDING),
                    invocation.<java.time.Instant>getArgument(3).plusSeconds(300)));
        }).when(ownership).claim(anyLong(), any(), any(), any());

        service.pollAndRun();

        verify(statusRepository).settleInconsistentDay(eq(1L), eq(D1), anyString(), anyString(),
                any());
        verify(dayProcessor).process(argThat(l -> l.businessDay().equals(D3)), any());
    }

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

    /**
     * Una giornata gia' ingerita da un'altra richiesta non viene riscritta.
     *
     * <p><strong>Il difetto che questo test impedisce di reintrodurre.</strong> Il BOOTSTRAP si crea
     * righe di stato proprie, quindi per lui una giornata gia' fatta dal DAILY risulta nuova:
     * {@code RESUME} non saltava nulla e la scrittura, che e' un INSERT fedele senza deduplica, ne
     * <em>raddoppiava</em> le righe. Su una giornata reale sono 810.492 righe che diventano
     * 1.620.984, e nessun contatore lo segnala: X ed N descrivono questa elaborazione, non la
     * tabella.
     *
     * <p>Si verifica sul lease consegnato al processore, perche' e' li' che la decisione diventa
     * effettiva: {@code wasAlreadyDone} deve rispondere di si' per il dataset gia' presente.
     */
    @Test
    void ilBootstrapNonRiscriveUnaGiornataGiaIngeritaDaUnAltraRichiesta() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(bootstrap(1L, D1, D1)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(statusRepository.alreadyIngestedElsewhere(eq(1L), eq(D1)))
                .thenReturn(java.util.EnumSet.allOf(FdrDataset.class));
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        ArgumentCaptor<FdrDayLease> lease = ArgumentCaptor.forClass(FdrDayLease.class);
        verify(dayProcessor).process(lease.capture(), eq(FdrDayScope.RESUME));
        assertThat(lease.getValue().wasAlreadyDone(FdrDataset.FLUSSI_RENDICONTAZIONE)).isTrue();
        assertThat(lease.getValue().wasAlreadyDone(FdrDataset.IUV_RENDICONTATI)).isTrue();
    }

    /**
     * Una giornata presente a meta' si completa, non si salta e non si raddoppia.
     *
     * <p>E' il caso che obbliga alla granularita' del dataset: i due si elaborano in sequenza,
     * quindi un guasto a meta' lascia FLUSSI concluso e IUV no. Saltare l'intera giornata
     * lascerebbe gli IUV mancanti per sempre; non saltare nulla duplicherebbe i FLUSSI.
     */
    @Test
    void unaGiornataPresenteAMetaSiCompletaSoloPerLaParteMancante() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(bootstrap(1L, D1, D1)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(statusRepository.alreadyIngestedElsewhere(eq(1L), eq(D1)))
                .thenReturn(java.util.EnumSet.of(FdrDataset.FLUSSI_RENDICONTAZIONE));
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        ArgumentCaptor<FdrDayLease> lease = ArgumentCaptor.forClass(FdrDayLease.class);
        verify(dayProcessor).process(lease.capture(), eq(FdrDayScope.RESUME));
        assertThat(lease.getValue().wasAlreadyDone(FdrDataset.FLUSSI_RENDICONTAZIONE))
                .as("gia' scritto altrove: va saltato")
                .isTrue();
        assertThat(lease.getValue().wasAlreadyDone(FdrDataset.IUV_RENDICONTATI))
                .as("mancante: va elaborato, altrimenti resterebbe assente per sempre")
                .isFalse();
    }

    /**
     * La REINGESTION non consulta la presenza, ed e' corretto cosi'.
     *
     * <p>Svuota le due partizioni prima di riscrivere: saltare un dataset perche' "risulta gia'
     * presente" lascerebbe la giornata <strong>senza</strong> quei dati, che e' l'esito peggiore.
     */
    @Test
    void laReingestioneNonConsultaLaPresenzaDiAltreRichieste() {
        when(batchRepository.findPendingCommands())
                .thenReturn(List.of(new FdrBatchRepository.FdrBatchRow(1L, FdrWorkflow.REINGESTION, null, null)));
        when(batchRepository.claim(anyLong(), any())).thenReturn(true);
        when(batchRepository.findBusinessDays(1L)).thenReturn(List.of(D1));
        when(batchRepository.findDatasets(1L, D1)).thenReturn(List.of(
                FdrDataset.FLUSSI_RENDICONTAZIONE.name(), FdrDataset.IUV_RENDICONTATI.name()));
        when(preparer.preflight(any())).thenReturn(rebuildable());
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service.pollAndRun();

        verify(statusRepository, never()).alreadyIngestedElsewhere(anyLong(), any());
        verify(dayProcessor).process(any(), eq(FdrDayScope.REBUILD));
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
