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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.inOrder;
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
        verify(dayProcessor, never()).process(anyLong(), any());
    }

    // ─── Claim ───────────────────────────────────────────────────────────────────

    /**
     * Il claim copre la giornata intera, entrambi i dataset in un colpo.
     *
     * <p>Una condizione per dataset permetterebbe a un pod di prendere i FLUSSI e a un altro gli
     * IUV della stessa giornata: per una reingestione significherebbe due {@code TRUNCATE}
     * intrecciati alle rispettive riscritture.
     */
    @Test
    void ilClaimPrendeLaGiornataInteraNonIlSingoloDataset() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.DAILY)));
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any())).thenReturn(2);
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service(new FdrProperties()).pollAndRetry();

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).update(sql.capture(), args.capture(), args.capture(), args.capture(),
                args.capture(), args.capture());

        assertThat(sql.getValue())
                .as("la condizione copre la giornata, senza filtrare il dataset")
                .contains("BATCH_ID = ? AND BUSINESS_DAY = ?")
                .doesNotContain("DATASET");
        assertThat(args.getAllValues()).contains(FdrProcessingStatus.PROCESSING.name(),
                1L, Date.valueOf(DAY), FdrProcessingStatus.WAITING_RETRY.name());
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
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any())).thenReturn(0);

        assertThat(service(new FdrProperties()).pollAndRetry()).isZero();
        verify(dayProcessor, never()).process(anyLong(), any());
    }

    // ─── Forma della ripresa ─────────────────────────────────────────────────────

    @Test
    void unaGiornataNormaleVieneSemplicementeRielaborata() {
        dueCandidates(List.of(candidate(1L, FdrWorkflow.DAILY)));
        claimSucceeds();
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        assertThat(service(new FdrProperties()).pollAndRetry()).isEqualTo(1);

        verify(dayProcessor).process(1L, DAY);
        verify(preparer, never()).guardAndTruncate(any());
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
        when(dayProcessor.process(anyLong(), any())).thenReturn(result(FdrProcessingStatus.COMPLETED));

        service(new FdrProperties()).pollAndRetry();

        InOrder order = inOrder(preparer, dayProcessor);
        order.verify(preparer).preflight(DAY);
        order.verify(preparer).guardAndTruncate(DAY);
        order.verify(dayProcessor).process(1L, DAY);
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

        verify(preparer, never()).guardAndTruncate(any());
        verify(dayProcessor, never()).process(anyLong(), any());
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
        verify(preparer, never()).guardAndTruncate(any());
        verify(dayProcessor, never()).process(anyLong(), any());
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
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(), any()))
                .thenReturn(1);
        when(preparer.preflight(DAY)).thenThrow(new IllegalStateException("azure unreachable"));

        service(new FdrProperties()).pollAndRetry();

        verify(statusRepository).settleDay(eq(1L), eq(DAY),
                eq(FdrProcessingStatus.PROCESSING), eq(FdrProcessingStatus.WAITING_RETRY),
                eq("IllegalStateException"), anyString(),
                eq(NOW.plus(new FdrProperties().getRetry().getInterval())), any());
        verify(preparer, never()).guardAndTruncate(any());
    }

    /** Esauriti i tentativi, lo stesso guasto chiude la giornata invece di riproporla. */
    @Test
    void esauritiITentativiUnGuastoTecnicoDiventaTerminale() {
        FdrProperties properties = new FdrProperties();
        properties.getRetry().setMaxAttempts(3);
        dueCandidates(List.of(candidate(1L, FdrWorkflow.REINGESTION)));
        claimSucceeds();
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), any(), any()))
                .thenReturn(3);
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
                new FdrRetryPolicy(properties), statusRepository, schema, properties,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @SuppressWarnings("unchecked")
    private void dueCandidates(List<FdrRetryService.FdrRetryCandidate> candidates) {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any()))
                .thenReturn((List) candidates);
    }

    private void claimSucceeds() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any())).thenReturn(2);
    }

    private static FdrRetryService.FdrRetryCandidate candidate(long batchId, FdrWorkflow workflow) {
        return new FdrRetryService.FdrRetryCandidate(batchId, DAY, workflow);
    }

    private static FdrDayResult result(FdrProcessingStatus status) {
        Map<FdrDataset, FdrDatasetResult> datasets = new EnumMap<>(FdrDataset.class);
        return new FdrDayResult(1L, DAY, status, datasets, false);
    }

}
