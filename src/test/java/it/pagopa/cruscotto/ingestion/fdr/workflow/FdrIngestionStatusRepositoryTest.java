package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Lo stato operativo di una giornata, e il modo in cui si chiude quando l'elaborazione non parte.
 *
 * <p>Il percorso normale è coperto dai test del processore di giornata. Qui ci sono i due casi che
 * quello non vede: la riga appena registrata da una richiesta amministrativa, che nessuno ha ancora
 * preso, e la chiusura di una giornata rifiutata <em>prima</em> dell'elaborazione — dove l'errore
 * non è perdere un dato ma lasciarne uno che sembra ancora da fare.
 */
class FdrIngestionStatusRepositoryTest {

    private static final long BATCH = 7L;
    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final Instant NOW = Instant.parse("2026-11-25T10:00:00Z");

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final FdrIngestionStatusRepository repository = newRepository();

    private FdrIngestionStatusRepository newRepository() {
        DbSchemaConfig schema = new DbSchemaConfig();
        schema.setSchema("sert_ingestor");
        return new FdrIngestionStatusRepository(jdbcTemplate, schema);
    }

    // ─── Lettura ─────────────────────────────────────────────────────────────────

    /**
     * Una riga appena registrata da una richiesta amministrativa deve poter essere riletta.
     *
     * <p>Non è un caso di scuola: il command poller crea gli status in {@code PENDING} e la prima
     * cosa che il processore di giornata fa è rileggerli, per decidere se c'è qualcosa da fare.
     * Finché {@code PENDING} non è stato un valore dell'enum, quella rilettura sollevava
     * un'eccezione e <strong>ogni giornata di un BOOTSTRAP si fermava al primo passo</strong> — un
     * difetto che i mock dei test di servizio non potevano vedere, perché il valore non passava mai
     * da {@code valueOf}.
     */
    @Test
    void unaGiornataRegistrataEnonAncoraPresaSiRilegge() {
        givenRow(FdrProcessingStatus.PENDING.name(), 0);

        assertThatCode(() -> repository.find(BATCH, DAY, FdrDataset.IUV_RENDICONTATI))
                .doesNotThrowAnyException();
        assertThat(repository.find(BATCH, DAY, FdrDataset.IUV_RENDICONTATI))
                .get()
                .extracting(FdrIngestionStatusRepository.FdrStatusRow::status)
                .isEqualTo(FdrProcessingStatus.PENDING);
    }

    /** E non è terminale: una giornata in attesa non va considerata chiusa con successo. */
    @Test
    void unaGiornataInAttesaNonEunEsitoTerminaleDiSuccesso() {
        assertThat(FdrProcessingStatus.PENDING.isTerminalSuccess()).isFalse();
        assertThat(FdrProcessingStatus.PROCESSING.isTerminalSuccess()).isFalse();
        assertThat(FdrProcessingStatus.COMPLETED.isTerminalSuccess()).isTrue();
        assertThat(FdrProcessingStatus.COMPLETED_WITH_WARNING.isTerminalSuccess()).isTrue();
    }

    // ─── Presa in carico e conteggio dei tentativi ───────────────────────────────

    // I tre test sulla presa in carico vivevano qui finche' era questo repository a scriverla.
    // Ora la presa in carico e' un claim di COPPIA - entrambe le righe della giornata, stesso
    // token, stesso tentativo - e sta in FdrDayOwnership, dove sono migrati insieme alle regole
    // che prima non esistevano: eleggibilita' dello stato, lease scaduto, nessun claim parziale.
    // Vedi FdrDayOwnershipTest.


    // ─── Chiusura di una giornata non elaborata ──────────────────────────────────

    /**
     * La chiusura copre la giornata intera, è condizionata sullo stato atteso e consuma un tentativo.
     *
     * <p>Tre proprietà in un solo statement, e ognuna risponde a un modo di sbagliare: senza il
     * filtro sullo stato atteso un pod potrebbe muovere una giornata che un altro sta elaborando;
     * con un filtro sul dataset ne muoverebbe metà; e senza l'incremento del tentativo un guasto
     * che si ripresenta prima dell'elaborazione — dove {@code ATTEMPT_COUNT} non viene toccato —
     * farebbe riprendere la stessa giornata all'infinito.
     */
    @Test
    void laChiusuraCopreLaGiornataIntraEdEcondizionataSulloStatoAtteso() {
        repository.settleDay(BATCH, DAY, FdrProcessingStatus.PENDING, FdrProcessingStatus.FAILED,
                "EMPTY_DATASET", "no data row in any file", null, NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).update(sql.capture(), args.capture(), args.capture(), args.capture(),
                args.capture(), args.capture(), args.capture(), args.capture(), args.capture());

        assertThat(sql.getValue())
                .contains("ATTEMPT_COUNT = ATTEMPT_COUNT + 1")
                .contains("WHERE BATCH_ID = ? AND BUSINESS_DAY = ? AND STATUS = ?")
                .as("l'unita' e' la giornata: filtrare il dataset ne muoverebbe meta'")
                .doesNotContain("DATASET");
        assertThat(args.getAllValues()).containsSubsequence(
                FdrProcessingStatus.FAILED.name(), BATCH, Date.valueOf(DAY),
                FdrProcessingStatus.PENDING.name());
    }

    /**
     * Su un esito terminale non va programmato alcun tentativo successivo.
     *
     * <p>Una data lì dentro farebbe riprendere per sempre una giornata che nessuno vuole più
     * ritentare: è lo stesso presidio che il core applica quando i tentativi si esauriscono.
     */
    @Test
    void unEsitoTerminaleNonProgrammaAlcunTentativo() {
        repository.settleDay(BATCH, DAY, FdrProcessingStatus.PROCESSING,
                FdrProcessingStatus.FAILED, "RETRY_EXHAUSTED", "no attempt left", null, NOW);

        assertThat(capturedArgs()).contains((Object) null);
    }

    @Test
    void unEsitoRitentabilePortaLOrarioDelTentativoSuccessivo() {
        Instant next = NOW.plusSeconds(3600);

        repository.settleDay(BATCH, DAY, FdrProcessingStatus.PROCESSING,
                FdrProcessingStatus.WAITING_RETRY, "Timeout", "azure unreachable", next, NOW);

        assertThat(capturedArgs()).contains(Timestamp.from(next));
    }

    // ─── Rilascio del possesso ───────────────────────────────────────────────────

    /**
     * Scrivere l'esito di un dataset <strong>non</strong> libera il possesso.
     *
     * <h2>Questo test asseriva il contrario, ed era il difetto</h2>
     *
     * <p>La prima stesura azzerava token e lease dentro {@code complete()}, "nella stessa UPDATE
     * dell'esito", e questo test lo fissava come requisito. Era sbagliato di un livello: il
     * possesso è della <strong>giornata</strong>, {@code complete()} è del <strong>dataset</strong>.
     * I due dataset si elaborano in sequenza, quindi liberare al primo toglieva il token anche
     * all'altro e il secondo moriva con una perdita di lease inventata — nessuna giornata poteva
     * completarsi.
     *
     * <p>È un promemoria su cosa sia un buon test: quello vecchio ancorava una <em>forma</em>
     * ("tutto in una UPDATE") invece del <em>perché</em>, e così rendeva più difficile correggere
     * la decisione che descriveva. Il rilascio ora è in {@link
     * FdrIngestionStatusRepository#releaseOwnership}, una volta per giornata.
     */
    @Test
    void laScritturaDellEsitoDiUnDatasetNonLiberaIlPossesso() {
        repository.complete(42L, FdrDatasetResult.of(FdrDataset.IUV_RENDICONTATI,
                FdrProcessingStatus.COMPLETED, 100, 100, null, null), NOW, null);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any(), any(), any(), any(), any(),
                any(), any(), any());
        assertThat(sql.getValue())
                .as("il possesso e' della giornata: liberarlo qui lascerebbe senza token la riga "
                        + "del dataset ancora in lavorazione")
                .doesNotContain("PROCESSING_TOKEN = NULL")
                .doesNotContain("LEASE_EXPIRES_AT = NULL")
                .doesNotContain("PROCESSING_INSTANCE_ID = NULL");
    }

    /**
     * Il rilascio di giornata tocca entrambe le righe, a qualunque esito siano arrivate.
     *
     * <p>Nessuna condizione sullo stato: i due dataset possono finire diversi — uno
     * {@code COMPLETED}, l'altro {@code COMPLETED_WITH_WARNING} — e filtrare su uno solo ne
     * libererebbe una sola. La condizione sul token invece c'è, ed è necessaria: se nel frattempo
     * avessimo perso il possesso, senza quella staremmo azzerando il lease di chi è subentrato.
     */
    @Test
    void ilRilascioDiGiornataToccaEntrambeLeRigheSenzaGuardareLoStato() {
        repository.releaseOwnership(BATCH, DAY, "token-1", NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any(), any());
        assertThat(sql.getValue())
                .contains("PROCESSING_TOKEN = NULL")
                .contains("LEASE_EXPIRES_AT = NULL")
                .contains("PROCESSING_INSTANCE_ID = NULL")
                .contains("WHERE BATCH_ID = ? AND BUSINESS_DAY = ? AND PROCESSING_TOKEN = ?")
                .doesNotContain("STATUS = ?")
                .doesNotContain("DATASET");
    }

    /**
     * Il ripristino di un dataset saltato riscrive lo stato e <strong>non i contatori</strong>.
     *
     * <p>Il claim porta in {@code PROCESSING} entrambe le righe, anche quella di un dataset già
     * concluso; saltarlo è giusto, ma la riga va rimessa al suo esito o resterebbe in lavorazione
     * per sempre. I contatori però sono quelli veri del tentativo riuscito: il risultato che la
     * ripresa si porta dietro li ha a zero, e riscriverli cancellerebbe l'unica traccia di quante
     * righe erano state davvero ingerite.
     */
    @Test
    void ilRipristinoNonToccaIContatori() {
        repository.restorePreviousOutcome(42L, FdrProcessingStatus.COMPLETED, NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any());
        assertThat(sql.getValue())
                .contains("SET STATUS = ?")
                .doesNotContain("ROWS_SOURCE")
                .doesNotContain("ROWS_PROCESSED")
                .doesNotContain("ROWS_FAILED");
    }

    /**
     * Vale anche per {@code WAITING_RETRY}, e non è una sfumatura.
     *
     * <p>Una giornata che aspetta un nuovo tentativo <em>non è in lavorazione</em>: lasciarle
     * addosso un lease impedirebbe a chiunque di riprenderla fino alla scadenza, cioè il poller dei
     * retry la troverebbe e non potrebbe farne nulla.
     */
    @Test
    void ancheUnEsitoRitentabileLiberaIlPossesso() {
        repository.settleDay(BATCH, DAY, FdrProcessingStatus.PROCESSING,
                FdrProcessingStatus.WAITING_RETRY, "Timeout", "azure unreachable",
                NOW.plusSeconds(3600), NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any(), any(), any(), any(), any(),
                any());
        assertThat(sql.getValue())
                .contains("PROCESSING_TOKEN = NULL")
                .contains("LEASE_EXPIRES_AT = NULL")
                .contains("PROCESSING_INSTANCE_ID = NULL");
    }

    /**
     * Un messaggio più lungo della colonna viene accorciato invece di far fallire lo statement.
     *
     * <p>È testo che scriviamo noi, non un valore del sorgente: qui accorciare è legittimo, ed è
     * l'opposto della regola che vale sulle tabelle business, dove un valore troppo lungo deve far
     * emergere il problema.
     */
    @Test
    void unMessaggioTroppoLungoVieneAccorciato() {
        repository.settleDay(BATCH, DAY, FdrProcessingStatus.PROCESSING,
                FdrProcessingStatus.FAILED, "Kind", "x".repeat(4_000), null, NOW);

        boolean withinLimit = capturedArgs().stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .allMatch(value -> value.length() <= 1_000);
        assertThat(withinLimit).isTrue();
    }

    // ─── Utilità ─────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void givenNoRow() {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any()))
                .thenReturn(List.of());
    }

    @SuppressWarnings("unchecked")
    private void givenRow(String status, int attemptCount) {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any()))
                .thenAnswer(invocation -> List.of(
                        new FdrIngestionStatusRepository.FdrStatusRow(1L,
                                FdrProcessingStatus.valueOf(status), attemptCount)));
    }

    private List<Object> capturedArgs() {
        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).update(anyString(), args.capture(), args.capture(), args.capture(),
                args.capture(), args.capture(), args.capture(), args.capture(), args.capture());
        return args.getAllValues();
    }

    // ─── Giornate gia' ingerite da un'altra richiesta ────────────────────────────

    /**
     * La domanda che impedisce al BOOTSTRAP di raddoppiare una giornata.
     *
     * <p>Si verifica la <em>forma</em> dello statement, perche' e' li' che vivono le due condizioni
     * che contano: l'esclusione del batch corrente e il filtro sui soli esiti terminali positivi.
     * Un test che si limitasse al valore di ritorno passerebbe anche con una query che risponde
     * guardando la richiesta stessa, cioe' che risponde su cio' che si sta facendo adesso.
     */
    @Test
    void laDomandaSuiGiorniGiaIngeritiEscludeLaRichiestaCorrente() {
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), any(), any(), any(), any()))
                .thenReturn(List.of(FdrDataset.FLUSSI_RENDICONTAZIONE.name()));

        assertThat(repository.alreadyIngestedElsewhere(BATCH, DAY))
                .containsExactly(FdrDataset.FLUSSI_RENDICONTAZIONE);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).queryForList(sql.capture(), eq(String.class), any(), any(), any(), any());
        assertThat(sql.getValue())
                .as("senza l'esclusione risponderebbe con cio' che questa richiesta sta facendo")
                .contains("BATCH_ID <> ?")
                .contains("BUSINESS_DAY = ?")
                .contains("STATUS IN (?, ?)");
    }

    /**
     * Uno stato scritto da altri che l'enum non conosce non deve far cadere la giornata.
     *
     * <p>La colonna non ha un CHECK, quindi il database accetterebbe qualunque stringa. Questa e'
     * una domanda <em>accessoria</em>: nel dubbio si elabora, che al massimo costa una
     * duplicazione gia' possibile prima, mentre sollevare impedirebbe un BOOTSTRAP legittimo.
     */
    @Test
    void unDatasetSconosciutoVieneIgnoratoInveceDiFarFallire() {
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), any(), any(), any(), any()))
                .thenReturn(List.of("QUALCOSA_DI_IGNOTO", FdrDataset.IUV_RENDICONTATI.name()));

        assertThat(repository.alreadyIngestedElsewhere(BATCH, DAY))
                .containsExactly(FdrDataset.IUV_RENDICONTATI);
    }
}
