package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * La ripresa delle giornate il cui tentativo successivo e' scaduto.
 *
 * <p>La distinzione che questi test proteggono e' una sola, e si perde facilmente: si
 * <strong>polla</strong> ogni pochi minuti, si <strong>ritenta</strong> dopo un'ora. Chi
 * confondesse le due cose farebbe dodici tentativi all'ora invece di uno, esaurendo i cinque
 * disponibili in venticinque minuti — e la giornata diventerebbe terminale prima che il guasto
 * transitorio abbia avuto il tempo di passare.
 */
class FdrRetryServiceTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final Instant NOW = Instant.parse("2026-11-18T10:00:00Z");

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final FdrDayProcessor dayProcessor = mock(FdrDayProcessor.class);
    private final FdrReingestionPreparer preparer = mock(FdrReingestionPreparer.class);
    private final FdrDayOwnership ownership = mock(FdrDayOwnership.class);
    private final FdrIngestionEventRepository eventRepository =
            mock(FdrIngestionEventRepository.class);

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

    // ─── Selezione delle giornate scadute ────────────────────────────────────────

    /**
     * Le tre condizioni della query, verificate sui parametri effettivi.
     *
     * <p>Sono asserzioni sui valori passati e non sul testo del SQL di proposito: il testo si puo'
     * riscrivere, i parametri no — se uno sparisse, la selezione cambierebbe di significato
     * (riprendere giornate non ancora scadute, o giornate che hanno gia' esaurito i tentativi) senza
     * che nulla fallisca.
     */
    @Test
    void siSelezionanoSoloLeGiornateScaduteConTentativiRimasti() {
        FdrProperties properties = new FdrProperties();
        service(properties).findDue(NOW);

        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).query(anyString(), any(RowMapper.class), args.capture(),
                args.capture(), args.capture());

        assertThat(args.getAllValues()).containsExactly(
                FdrProcessingStatus.WAITING_RETRY.name(),
                Timestamp.from(NOW),
                properties.getRetry().getMaxAttempts());
    }

    /**
     * Il tetto sui tentativi e' una property, non un numero scritto nel codice.
     *
     * <p>Il requisito dice cinque, ma un documento intermedio diceva uno: se fosse nel codice, la
     * divergenza si scoprirebbe soltanto vedendo una giornata chiudersi al primo errore.
     */
    @Test
    void ilTettoSuiTentativiVieneDallaConfigurazione() {
        FdrProperties properties = new FdrProperties();
        properties.getRetry().setMaxAttempts(3);

        service(properties).findDue(NOW);

        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).query(anyString(), any(RowMapper.class), args.capture(),
                args.capture(), args.capture());
        assertThat(args.getAllValues()).contains(3);
        assertThat(new FdrProperties().getRetry().getMaxAttempts())
                .as("il default resta quello confermato dal cliente")
                .isEqualTo(5);
    }

    /** L'intervallo fra due tentativi e il ritmo del poller sono due valori distinti. */
    @Test
    void lIntervalloDiRetryNonCoincideColRitmoDelPoller() {
        FdrProperties properties = new FdrProperties();

        assertThat(properties.getRetry().getInterval()).isEqualTo(Duration.ofHours(1));
        assertThat(properties.getRetry().getPollCron()).isNotBlank();
        assertThat(properties.getRetry().getPollCron())
                .as("un poll orario renderebbe il tentativo puntuale a meno di un'ora di ritardo")
                .isNotEqualTo("0 0 * * * ?");
    }

    @Test
    void senzaGiornateScaduteNonSiFaNulla() {
        dueCandidates(List.of());

        assertThat(service(new FdrProperties()).pollAndRetry()).isZero();
        verify(dayProcessor, never()).process(any(), any());
    }

    // ─── Claim ───────────────────────────────────────────────────────────────────

    /**
     * La presa in carico passa dall'ownership, e riguarda la <strong>giornata</strong>.
     *
     * <p>Il retry non ha piu' un claim proprio: ne aveva uno che portava la riga in
     * {@code PROCESSING} senza assegnare token ne' lease, quindi dichiarava la presa in carico
     * senza renderla dimostrabile a chi scriveva dopo. Ora delega, e l'unita' resta la giornata —
     * una condizione per dataset permetterebbe a un pod di prendere i FLUSSI e a un altro gli IUV,
     * che per una reingestione significa due {@code TRUNCATE} intrecciati alle riscritture.
     *
     * <p>Che il claim sia atomico sulla coppia e' verificato dove vive, in
     * {@link FdrDayOwnershipTest}: qui si verifica soltanto che il retry non se ne faccia uno suo.
     */
    @Test
    void laPresaInCaricoEDelegataAllOwnershipEdEDiGiornata() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.DAILY)));
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service(new FdrProperties()).pollAndRetry();

        verify(ownership).claim(eq(1L), eq(DAY),
                eq(java.util.EnumSet.of(FdrProcessingStatus.WAITING_RETRY)), any());
    }

    /**
     * Il claim perso significa che un altro pod ha gia' preso la giornata.
     *
     * <p>La transizione e' un'{@code UPDATE} condizionata sullo stato precedente, quindi fra due
     * tentativi simultanei uno solo vede righe aggiornate. Chi perde passa oltre — riprendere lo
     * stesso lavoro raddoppierebbe la scrittura.
     */
    @Test
    void unaGiornataGiaPresaDaUnAltroPodNonVieneRiprocessata() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.DAILY)));
        // Il claim non concede la giornata: un altro pod e' arrivato prima.
        doReturn(java.util.Optional.empty()).when(ownership).claim(anyLong(), any(), any(), any());

        assertThat(service(new FdrProperties()).pollAndRetry()).isZero();
        verify(dayProcessor, never()).process(any(), any());
    }

    // ─── Forma della ripresa ─────────────────────────────────────────────────────

    @Test
    void unaGiornataNormaleVieneSemplicementeRielaborata() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.DAILY)));
        claimSucceeds();
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        assertThat(service(new FdrProperties()).pollAndRetry()).isEqualTo(1);

        verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(DAY)), any());
        verify(preparer, never()).guardAndTruncate(any(), any());
    }

    /**
     * La ripresa di una reingestione rifa' tutto: preflight, truncate delle due partizioni,
     * elaborazione.
     *
     * <p>Non si riparte da meta'. Il tentativo precedente potrebbe aver gia' svuotato la giornata e
     * riscritto solo una parte, e senza un checkpoint a livello di file nessuno sa quale:
     * ricostruire e' l'unica operazione di cui si conosce il risultato.
     */
    @Test
    void laRipresaDiUnaReingestionRifaLInteraGiornata() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.REINGESTION)));
        claimSucceeds();
        when(preparer.preflight(DAY)).thenReturn(
                new FdrReingestionPreparer.FdrPreflightResult(true, null, null, null));
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service(new FdrProperties()).pollAndRetry();

        InOrder order = inOrder(preparer, dayProcessor);
        order.verify(preparer).preflight(DAY);
        order.verify(preparer).guardAndTruncate(eq(DAY), any());
        order.verify(dayProcessor).process(argThat(l -> l.batchId() == 1L && l.businessDay().equals(DAY)), any());
    }

    /**
     * Se nel frattempo la sorgente non e' piu' ricostruibile, la ripresa si ferma prima del
     * {@code TRUNCATE}: meglio una giornata vecchia che una vuota.
     */
    @Test
    void laRipresaSiFermaSeLaSorgenteNonEpiuRicostruibile() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.REINGESTION)));
        claimSucceeds();
        when(preparer.preflight(DAY)).thenReturn(
                new FdrReingestionPreparer.FdrPreflightResult(false, FdrDataset.IUV_RENDICONTATI,
                        "DATASET_MISSING", "gone"));

        service(new FdrProperties()).pollAndRetry();

        verify(preparer, never()).guardAndTruncate(any(), any());
        verify(dayProcessor, never()).process(any(), any());
    }

    // ─── Nessuna giornata appesa ─────────────────────────────────────────────────

    /**
     * Sorgente non piu' ricostruibile: esito terminale, non una giornata ferma.
     *
     * <p>La giornata e' gia' reclamata ({@code PROCESSING}) e nessun poller guarda quello stato per
     * una REINGESTION. Uscire senza riscriverlo la lascerebbe ferma per sempre, apparentemente in
     * lavorazione - ed e' il difetto che questo test impedisce di reintrodurre.
     */
    @Test
    void unaSorgenteNonPiuRicostruibileChiudeLaGiornataInveceDiLasciarlaAppesa() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.REINGESTION)));
        claimSucceeds();
        when(preparer.preflight(DAY)).thenReturn(
                new FdrReingestionPreparer.FdrPreflightResult(false, FdrDataset.IUV_RENDICONTATI,
                        "DATASET_MISSING", "no file published for this day"));

        service(new FdrProperties()).pollAndRetry();

        verify(statusRepository).settleDay(eq(1L), eq(DAY),
                eq(FdrProcessingStatus.PROCESSING), eq(FdrProcessingStatus.FAILED),
                eq("DATASET_MISSING"), anyString(), isNull(), any());
        verify(preparer, never()).guardAndTruncate(any(), any());
        verify(dayProcessor, never()).process(any(), any());
    }

    /**
     * Guasto tecnico in fase di preparazione: la giornata torna in attesa e <strong>consuma un
     * tentativo</strong>.
     *
     * <p>Il consumo non e' un dettaglio contabile. {@code ATTEMPT_COUNT} cresce dentro
     * l'elaborazione, che qui non viene mai raggiunta: senza l'incremento, un guasto persistente
     * (Azure irraggiungibile per un giorno) farebbe riprendere la stessa giornata ogni ora
     * all'infinito, perche' il filtro {@code ATTEMPT_COUNT < max} non scatterebbe mai.
     */
    @Test
    void unGuastoTecnicoRimetteLaGiornataInAttesaConsumandoUnTentativo() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.REINGESTION)));
        claimSucceeds();
        claimSucceedsAtAttempt(1);
        when(preparer.preflight(DAY)).thenThrow(new IllegalStateException("azure unreachable"));

        service(new FdrProperties()).pollAndRetry();

        verify(statusRepository).settleDay(eq(1L), eq(DAY),
                eq(FdrProcessingStatus.PROCESSING), eq(FdrProcessingStatus.WAITING_RETRY),
                eq("IllegalStateException"), anyString(),
                eq(NOW.plus(new FdrProperties().getRetry().getInterval())), any());
        verify(preparer, never()).guardAndTruncate(any(), any());
    }

    /** Esauriti i tentativi, lo stesso guasto chiude la giornata invece di riproporla. */
    @Test
    void esauritiITentativiUnGuastoTecnicoDiventaTerminale() {
        FdrProperties properties = new FdrProperties();
        properties.getRetry().setMaxAttempts(3);
        dueCandidates(List.of(candidate(1L, FdrWorkflow.REINGESTION)));
        claimSucceeds();
        claimSucceedsAtAttempt(3);
        when(preparer.preflight(DAY)).thenThrow(new IllegalStateException("azure unreachable"));

        service(properties).pollAndRetry();

        verify(statusRepository).settleDay(eq(1L), eq(DAY),
                eq(FdrProcessingStatus.PROCESSING), eq(FdrProcessingStatus.FAILED),
                anyString(), anyString(),
                isNull(), any());
    }

    // ─── Utilita' ────────────────────────────────────────────────────────────────

    private FdrRetryService service(FdrProperties properties) {
        DbSchemaConfig schema = new DbSchemaConfig();
        schema.setSchema("sert_ingestor");
        return new FdrRetryService(jdbcTemplate, dayProcessor, preparer,
                new FdrRetryPolicy(properties), statusRepository, eventRepository, schema,
                properties, ownership,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @SuppressWarnings("unchecked")

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
    void unaGiornataIncoerenteVieneChiusaEilPollerProsegue() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.DAILY)));
        doThrow(new FdrInconsistentDayException(1L, DAY, "FLUSSI_RENDICONTAZIONE=1 IUV_RENDICONTATI=4"))
                .when(ownership).claim(anyLong(), any(), any(), any());

        assertThat(service(new FdrProperties()).pollAndRetry())
                .as("una giornata chiusa per incoerenza non conta come ripresa")
                .isZero();

        verify(statusRepository).settleInconsistentDay(eq(1L), eq(DAY),
                eq(FdrInconsistentDayException.CODE), anyString(), any());
        verify(eventRepository).record(eq(1L), isNull(), eq(DAY), isNull(), isNull(),
                eq(FdrEventType.TECHNICAL_ERROR), eq(FdrEventType.ERROR),
                eq(FdrInconsistentDayException.CODE), anyString(), anyString(), any());
        verify(dayProcessor, never()).process(any(), any());
    }


    // ─── Giornate abbandonate ────────────────────────────────────────────────────

    /**
     * Il poller recupera anche le giornate il cui proprietario non c'e' piu'.
     *
     * <p><strong>Senza questa seconda selezione il recupero aveva un buco.</strong> Una giornata
     * DAILY interrotta da un pod morto non veniva ripresa da nessuno: il recupero a livello di
     * richiesta guarda soltanto BOOTSTRAP e REINGESTION — un batch DAILY non vi compare — e il
     * DAILY successivo crea un batch nuovo, lasciando le due righe del precedente ferme su
     * {@code PROCESSING} per sempre, con il loro batch.
     *
     * <p>Tre dettagli della query valgono l'asserzione: il raggruppamento per giornata (trattare
     * FLUSSI e IUV separatamente porterebbe due worker sulle due meta' della stessa giornata), il
     * lease <strong>nullo</strong> contato come scaduto (significa che nessuno puo' dimostrarne il
     * possesso, e il claim non lascia mai quella combinazione), e il tetto sui tentativi, senza il
     * quale una giornata esaurita verrebbe ripresa a ogni giro senza poter mai chiudersi.
     */
    @Test
    @SuppressWarnings("unchecked")
    void ilPollerCercaAncheLeGiornateAbbandonate() {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any(), any()))
                .thenReturn(List.of());

        service(new FdrProperties()).findAbandoned(NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(sql.capture(), any(RowMapper.class), any(), any(), any(), any());
        assertThat(sql.getValue())
                .contains("GROUP BY s.BATCH_ID, s.BUSINESS_DAY, b.WORKFLOW")
                .contains("s.LEASE_EXPIRES_AT IS NULL")
                .contains("s.ATTEMPT_COUNT < ?")
                .as("il tetto al lavoro di un giro: il resto viene ripreso fra cinque minuti")
                .contains("LIMIT ?");
    }

    /**
     * Una giornata abbandonata si riprende da {@code PROCESSING}, non da {@code WAITING_RETRY}.
     *
     * <p>E' l'unico punto in cui {@code PROCESSING} e' un punto di partenza legittimo, e non e' un
     * permesso pericoloso: il claim lo concede soltanto a lease scaduto, quindi una giornata ancora
     * viva resta intoccata anche se finisse per sbaglio in questa selezione.
     */
    @Test
    @SuppressWarnings("unchecked")
    void unaGiornataAbbandonataSiRiprendeDaProcessing() {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any()))
                .thenReturn(List.of());
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any(), any()))
                .thenReturn((List) List.of(candidate(1L, FdrWorkflow.DAILY)));
        when(dayProcessor.process(any(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        assertThat(service(new FdrProperties()).pollAndRetry()).isEqualTo(1);

        verify(ownership).claim(eq(1L), eq(DAY),
                eq(java.util.EnumSet.of(FdrProcessingStatus.PROCESSING)), any());
    }

    private void dueCandidates(List<FdrRetryService.FdrRetryCandidate> candidates) {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any()))
                .thenReturn((List) candidates);
    }

    /**
     * Il possesso concesso, al tentativo indicato.
     *
     * <p>Prima era una {@code UPDATE} locale di questo servizio, che portava la riga in
     * {@code PROCESSING} senza assegnare ne' token ne' lease: dichiarava la presa in carico senza
     * renderla dimostrabile a chi scriveva dopo. Ora e' un claim di coppia, e il numero di
     * tentativo viaggia nel possesso invece di essere riletto a parte.
     */
    private void claimSucceeds() {
        claimSucceedsAtAttempt(1);
    }

    private void claimSucceedsAtAttempt(int attemptNo) {
                // doAnswer e NON when(...): su un metodo gia' stubbato con un Answer, when(mock.metodo(...))
        // ESEGUE quell'Answer con argomenti nulli prima di registrare il nuovo stub. Qui la
        // giornata e l'istante arriverebbero null e l'Answer esploderebbe - che e' esattamente
        // cio' che succedeva. Vale per ogni ristubbatura di claim in questa classe.
        doAnswer(invocation ->
                java.util.Optional.of(new FdrDayLease(invocation.getArgument(0),
                        invocation.getArgument(1), "token-1", attemptNo,
                        java.util.Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, 1L,
                                FdrDataset.IUV_RENDICONTATI, 2L),
                        java.util.Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE,
                                FdrProcessingStatus.WAITING_RETRY,
                                FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.WAITING_RETRY),
                        invocation.<java.time.Instant>getArgument(3).plusSeconds(300)))).when(ownership).claim(anyLong(), any(), any(), any());
    }

    private static FdrRetryService.FdrRetryCandidate candidate(long batchId, FdrWorkflow workflow) {
        return new FdrRetryService.FdrRetryCandidate(batchId, DAY, workflow);
    }

    private static FdrDayResult result(FdrProcessingStatus status) {
        Map<FdrDataset, FdrDatasetResult> datasets = new EnumMap<>(FdrDataset.class);
        return new FdrDayResult(1L, DAY, status, datasets, false);
    }

}
