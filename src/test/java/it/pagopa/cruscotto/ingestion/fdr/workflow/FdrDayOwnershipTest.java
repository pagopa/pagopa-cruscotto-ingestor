package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Chi possiede una giornata, e come lo si dimostra.
 *
 * <h2>Cosa si verifica qui e cosa no</h2>
 *
 * <p>Qui si verificano le <strong>regole</strong>: quando una giornata è claimabile, che il claim
 * sia di coppia e mai parziale, che le due righe escano allineate, che il rinnovo pretenda due
 * righe. Il database è finto, quindi questi test <em>non</em> dimostrano l'atomicità né il
 * comportamento dei lock — quella è una proprietà di PostgreSQL e va provata su PostgreSQL, con due
 * sessioni vere. Lo script sta in {@code .handover/fdr-verifica-lease.sql}.
 *
 * <p>La distinzione è importante perché un test con un database finto che <em>sembrasse</em>
 * provare il fencing sarebbe peggio di nessun test: darebbe per garantito ciò che non lo è.
 */
class FdrDayOwnershipTest {

    private static final long BATCH = 7L;
    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final Instant NOW = Instant.parse("2026-11-18T10:00:00Z");

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final PlatformTransactionManager transactionManager =
            mock(PlatformTransactionManager.class);

    private FdrDayOwnership ownership;

    @BeforeEach
    void setUp() {
        // Una transazione finta che esegue il callback e basta: la TransactionTemplate vera ha
        // bisogno solo di questo per lasciar passare il corpo del claim.
        TransactionStatus status = new SimpleTransactionStatus();
        when(transactionManager.getTransaction(any())).thenReturn(status);

        DbSchemaConfig schema = new DbSchemaConfig();
        schema.setSchema("sert_ingestor");
        FdrProperties properties = new FdrProperties();
        properties.getRecovery().setLeaseDuration(Duration.ofMinutes(5));
        properties.getRecovery().setHeartbeatInterval(Duration.ofSeconds(30));

        ownership = new FdrDayOwnership(jdbcTemplate, transactionManager, schema, properties,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    // ─── Claim di coppia ─────────────────────────────────────────────────────────

    /**
     * Le due righe escono dal claim con lo stesso token, la stessa scadenza e lo stesso tentativo.
     *
     * <p>È l'invariante che PostgreSQL non può esprimere come {@code CHECK} — è una condizione fra
     * righe diverse — quindi è garantita da una sola {@code UPDATE} che le tocca entrambe. Il test
     * lo verifica dove la garanzia vive davvero: nella forma dello statement, non in due scritture
     * che <em>casualmente</em> portano lo stesso valore.
     */
    @Test
    @DisplayName("il claim porta entrambe le righe allo stesso token, lease e tentativo")
    void ilClaimAllineaLeDueRigheConUnaSolaUpdate() {
        givenLockedPair(FdrProcessingStatus.PENDING, 0, FdrProcessingStatus.PENDING, 0);
        givenUpdateTouches(2);

        FdrDayLease lease = ownership
                .claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PENDING), NOW)
                .orElseThrow();

        assertThat(lease.token()).isNotBlank();
        assertThat(lease.attemptNo()).isEqualTo(1);
        assertThat(lease.leaseExpiresAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));
        assertThat(lease.statusIdOf(FdrDataset.FLUSSI_RENDICONTAZIONE)).isEqualTo(1L);
        assertThat(lease.statusIdOf(FdrDataset.IUV_RENDICONTATI)).isEqualTo(2L);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any(), any(), any(), any(), any(),
                any(), any());
        assertThat(sql.getValue())
                .as("una sola UPDATE per la giornata: filtrare anche il dataset permetterebbe di "
                        + "aggiornarne una sola, cioe' una giornata posseduta a meta'")
                .contains("WHERE BATCH_ID = ? AND BUSINESS_DAY = ?")
                .doesNotContain("DATASET = ?");
    }

    /** Coppia coerente in ripresa: {@code 2, 2} diventa {@code 3}, su entrambe. */
    @Test
    @DisplayName("una coppia coerente avanza di uno")
    void unaCoppiaCoerenteAvanzaDiUno() {
        givenLockedPair(FdrProcessingStatus.WAITING_RETRY, 2, FdrProcessingStatus.WAITING_RETRY, 2);
        givenUpdateTouches(2);

        FdrDayLease lease = ownership
                .claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.WAITING_RETRY), NOW)
                .orElseThrow();

        assertThat(lease.attemptNo()).isEqualTo(3);
    }

    /**
     * Due righe che non concordano sul tentativo <strong>non</strong> vengono riallineate.
     *
     * <p>Questa è la correzione di una scelta sbagliata della prima stesura, che usava
     * {@code max(attempt) + 1}. Sembrava prudente — riallinea invece di conservare uno scarto — ma
     * con {@code FLUSSI=1} e {@code IUV=4} porta la giornata a <strong>5</strong>, cioè all'ultimo
     * tentativo disponibile: l'anomalia sparirebbe dalla vista <em>consumando</em> i tentativi che
     * servivano a recuperarla.
     *
     * <p>La divergenza non è uno stato da cui ripartire. I due dataset vengono tentati insieme,
     * sempre, da una sola {@code UPDATE}: se divergono, qualcosa li ha scritti fuori dal claim, e
     * finché non si sa cosa, proseguire è indovinare.
     */
    @Test
    @DisplayName("una coppia con tentativi divergenti viene rifiutata senza toccare nulla")
    void unaCoppiaDivergenteVieneRifiutata() {
        givenLockedPair(FdrProcessingStatus.WAITING_RETRY, 1, FdrProcessingStatus.WAITING_RETRY, 4);

        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> ownership.claim(BATCH, DAY,
                        EnumSet.of(FdrProcessingStatus.WAITING_RETRY), NOW))
                .isInstanceOf(FdrInconsistentDayException.class)
                .hasMessageContaining("FLUSSI_RENDICONTAZIONE=1")
                .hasMessageContaining("IUV_RENDICONTATI=4");

        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any(), any(), any(),
                any(), any(), any());
    }

    /** Lo stesso, a parti invertite: non dipende da quale riga porti il valore più alto. */
    @Test
    @DisplayName("la divergenza invertita si comporta allo stesso modo")
    void laDivergenzaInvertitaSiComportaAlloStessoModo() {
        givenLockedPair(FdrProcessingStatus.WAITING_RETRY, 4, FdrProcessingStatus.WAITING_RETRY, 1);

        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> ownership.claim(BATCH, DAY,
                        EnumSet.of(FdrProcessingStatus.WAITING_RETRY), NOW))
                .isInstanceOf(FdrInconsistentDayException.class);

        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any(), any(), any(),
                any(), any(), any());
    }

    /**
     * Esauriti i tentativi non si prende più nulla, e non si arriva a sei.
     *
     * <p>Il tetto lo applica chi seleziona le giornate — il poller filtra su
     * {@code ATTEMPT_COUNT < max} — quindi una coppia già a cinque non arriva nemmeno al claim. Il
     * test lo verifica dal lato che conta: se arrivasse, il claim non la ammetterebbe comunque,
     * perché lo stato terminale non è fra quelli eleggibili.
     */
    @Test
    @DisplayName("una giornata esaurita non produce un sesto tentativo")
    void unaGiornataEsauritaNonProduceUnSestoTentativo() {
        givenLockedPair(FdrProcessingStatus.FAILED, 5, FdrProcessingStatus.FAILED, 5);

        assertThat(ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.WAITING_RETRY), NOW))
                .isEmpty();
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any(), any(), any(),
                any(), any(), any());
    }

    /** Il primo tentativo reale è 1 e mai 0: un file con ATTEMPT_NO = 0 non deve poter esistere. */
    @Test
    @DisplayName("il primo tentativo e' uno, mai zero")
    void ilPrimoTentativoEuno() {
        givenLockedPair(FdrProcessingStatus.PENDING, 0, FdrProcessingStatus.PENDING, 0);
        givenUpdateTouches(2);

        assertThat(ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PENDING), NOW)
                .orElseThrow().attemptNo()).isEqualTo(1);
    }

    // ─── Coppie non valide ───────────────────────────────────────────────────────

    /**
     * Una riga sola non è una giornata: si rifiuta invece di adattarsi.
     *
     * <p>Proseguire vorrebbe dire indovinare cosa intendesse chi ha scritto quella richiesta, e una
     * giornata elaborata a metà è peggio di una non elaborata perché <em>sembra</em> a posto.
     */
    @Test
    @DisplayName("una giornata con una sola riga non viene presa in carico")
    void unaRigaSolaNonEunaGiornata() {
        givenLocked(List.<Object[]>of(row(1L, FdrDataset.FLUSSI_RENDICONTAZIONE,
                FdrProcessingStatus.PENDING, 0, null)));

        assertThat(ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PENDING), NOW))
                .isEmpty();
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any(), any(), any(),
                any(), any(), any());
    }

    /** Lo stesso dataset due volte: due righe, ma non la coppia. */
    @Test
    @DisplayName("un dataset ripetuto non viene preso in carico")
    void unDatasetRipetutoNonEunaCoppia() {
        givenLocked(List.<Object[]>of(
                row(1L, FdrDataset.FLUSSI_RENDICONTAZIONE, FdrProcessingStatus.PENDING, 0, null),
                row(2L, FdrDataset.FLUSSI_RENDICONTAZIONE, FdrProcessingStatus.PENDING, 0, null)));

        assertThat(ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PENDING), NOW))
                .isEmpty();
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any(), any(), any(),
                any(), any(), any());
    }

    /**
     * Se l'UPDATE non tocca entrambe le righe la transazione salta: non esiste un claim parziale.
     *
     * <p>Non può accadere — le righe sono bloccate da questa stessa transazione — ma se accadesse
     * significherebbe che qualcuno le sta cancellando sotto di noi, e proseguire scriverebbe dato
     * business su una giornata di cui non si sa più nulla.
     */
    @Test
    @DisplayName("un aggiornamento parziale fa saltare il claim invece di concederlo")
    void nessunClaimParziale() {
        givenLockedPair(FdrProcessingStatus.PENDING, 0, FdrProcessingStatus.PENDING, 0);
        givenUpdateTouches(1);

        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> ownership.claim(BATCH, DAY,
                        EnumSet.of(FdrProcessingStatus.PENDING), NOW))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1 status rows instead of 2");
    }

    // ─── Eleggibilita' e lease ───────────────────────────────────────────────────

    /** Uno stato fuori dall'insieme ammesso non si prende: è la guardia di idempotenza. */
    @Test
    @DisplayName("uno stato non ammesso non viene preso in carico")
    void unoStatoNonAmmessoNonSiPrende() {
        givenLockedPair(FdrProcessingStatus.COMPLETED, 1, FdrProcessingStatus.COMPLETED, 1);

        assertThat(ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PENDING), NOW))
                .isEmpty();
    }

    /**
     * Una giornata in lavorazione con il lease ancora valido è di qualcun altro: non si tocca.
     *
     * <p>È la sostituzione della vecchia domanda "esiste un pod FDR vivo?", che era troppo
     * grossolana: diceva che qualcuno stava lavorando, non che <em>questa</em> giornata fosse di
     * qualcuno.
     */
    @Test
    @DisplayName("una giornata con lease ancora valido non viene sottratta al suo proprietario")
    void unLeaseVivoProteggeLaGiornata() {
        Instant vivo = NOW.plusSeconds(60);
        givenLocked(List.<Object[]>of(
                row(1L, FdrDataset.FLUSSI_RENDICONTAZIONE, FdrProcessingStatus.PROCESSING, 1, vivo),
                row(2L, FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.PROCESSING, 1, vivo)));

        assertThat(ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PROCESSING), NOW))
                .isEmpty();
    }

    /** Scaduto il lease, la giornata è di nessuno e può essere ripresa. */
    @Test
    @DisplayName("una giornata con lease scaduto viene ripresa")
    void unLeaseScadutoLiberaLaGiornata() {
        Instant scaduto = NOW.minusSeconds(1);
        givenLocked(List.<Object[]>of(
                row(1L, FdrDataset.FLUSSI_RENDICONTAZIONE, FdrProcessingStatus.PROCESSING, 1,
                        scaduto),
                row(2L, FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.PROCESSING, 1, scaduto)));
        givenUpdateTouches(2);

        assertThat(ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PROCESSING), NOW))
                .isPresent();
    }

    /**
     * Un lease <strong>nullo</strong> su una riga in lavorazione conta come scaduto, non come
     * appena preso.
     *
     * <p>Significa che nessuno può dimostrarne il possesso, e il claim non lascia mai quella
     * combinazione — stato e lease li scrive la stessa UPDATE. Trattarlo come "vivo" stranirebbe la
     * giornata per sempre: nessun poller guarda {@code PROCESSING} se non per il lease scaduto.
     */
    @Test
    @DisplayName("una riga in lavorazione senza lease e' recuperabile, non intoccabile")
    void unLeaseNulloContaComeScaduto() {
        givenLocked(List.<Object[]>of(
                row(1L, FdrDataset.FLUSSI_RENDICONTAZIONE, FdrProcessingStatus.PROCESSING, 1, null),
                row(2L, FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.PROCESSING, 1, null)));
        givenUpdateTouches(2);

        assertThat(ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PROCESSING), NOW))
                .isPresent();
    }

    /**
     * Le righe vengono bloccate in ordine di dataset, e prima dell'aggiornamento.
     *
     * <p>L'ordine deterministico evita che due worker che bloccano le stesse due righe in sequenza
     * opposta si incastrino: PostgreSQL risolverebbe la cosa uccidendone uno con un deadlock invece
     * di farlo semplicemente aspettare.
     */
    @Test
    @DisplayName("le righe si bloccano in ordine di dataset, prima di aggiornarle")
    void leRigheSiBloccanoInOrdineDeterministico() {
        givenLockedPair(FdrProcessingStatus.PENDING, 0, FdrProcessingStatus.PENDING, 0);
        givenUpdateTouches(2);

        ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PENDING), NOW);

        ArgumentCaptor<String> select = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(select.capture(), any(RowMapper.class), any(), any());
        assertThat(select.getValue())
                .contains("ORDER BY DATASET")
                .contains("FOR UPDATE");
    }

    // ─── Rinnovo ─────────────────────────────────────────────────────────────────

    /**
     * Il rinnovo riesce solo se tocca <strong>entrambe</strong> le righe.
     *
     * <p>Una sola riga aggiornata è una giornata spezzata, e va trattata come perdita del possesso
     * e mai come successo parziale: chi proseguisse scriverebbe su metà giornata di cui non è più
     * padrone.
     */
    @Test
    @DisplayName("il rinnovo vale solo se tocca entrambe le righe")
    void ilRinnovoPretendeDueRighe() {
        FdrDayLease lease = someLease();

        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any()))
                .thenReturn(2);
        assertThat(ownership.renew(lease, NOW)).isTrue();

        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any()))
                .thenReturn(1);
        assertThat(ownership.renew(lease, NOW))
                .as("una riga sola non e' un successo a meta': e' una giornata spezzata")
                .isFalse();

        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any()))
                .thenReturn(0);
        assertThat(ownership.renew(lease, NOW)).isFalse();
    }

    /**
     * Il rinnovo richiede insieme lo stato e il token, e <strong>non</strong> tocca il tentativo.
     *
     * <p>Senza lo stato, un worker potrebbe rinnovare una giornata chiusa nel frattempo da un altro
     * percorso e continuare a scriverci dentro. E incrementare il tentativo a ogni battito
     * esaurirebbe i cinque disponibili in pochi minuti di lavoro perfettamente normale.
     */
    @Test
    @DisplayName("il rinnovo verifica stato e token e non consuma tentativi")
    void ilRinnovoNonConsumaTentativi() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any()))
                .thenReturn(2);

        ownership.renew(someLease(), NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any(), any(), any(), any());
        assertThat(sql.getValue())
                .contains("STATUS = ?")
                .contains("PROCESSING_TOKEN = ?")
                .doesNotContain("ATTEMPT_COUNT");
    }

    /**
     * Il battito è limitato dall'intervallo configurato, la chiusura no.
     *
     * <p>Sono due cose diverse: la chiusura è una garanzia di correttezza e non può essere saltata
     * per risparmiare una scrittura, il battito è manutenzione e ripeterlo ogni pochi millisecondi
     * non aggiunge nulla.
     */
    @Test
    @DisplayName("il battito rispetta l'intervallo, la chiusura e' sempre eseguita")
    void ilBattitoERallentatoMaLaChiusuraNo() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any()))
                .thenReturn(2);
        FdrLeaseGuard guard = ownership.guardFor(someLease());

        guard.renewIfDue(NOW.plusSeconds(5));
        verify(jdbcTemplate, never()).update(anyString(), any(), any(), any(), any(), any(), any());

        guard.renewIfDue(NOW.plusSeconds(31));
        verify(jdbcTemplate).update(anyString(), any(), any(), any(), any(), any(), any());

        guard.requireOwnership();
        guard.requireOwnership();
        verify(jdbcTemplate, org.mockito.Mockito.times(3))
                .update(anyString(), any(), any(), any(), any(), any(), any());
    }

    /** Perso il possesso, la chiusura solleva invece di lasciar scrivere. */
    @Test
    @DisplayName("la chiusura solleva quando il possesso e' perso")
    void laChiusuraSollevaQuandoIlPossessoEperso() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any()))
                .thenReturn(0);
        FdrLeaseGuard guard = ownership.guardFor(someLease());

        org.assertj.core.api.Assertions.assertThatThrownBy(guard::requireOwnership)
                .isInstanceOf(it.pagopa.cruscotto.ingestion.fdr.persistence.FdrLeaseLostException.class)
                .hasMessageContaining("another worker owns it now");
    }

    // ─── PROCESSING_INSTANCE_ID ──────────────────────────────────────────────────

    /**
     * Il claim scrive l'identificativo del processo, lo stesso sulle due righe.
     *
     * <p>Non è ownership — quella è token più lease — è la risposta a "chi devo andare a guardare".
     * Serve perché in produzione non c'è accesso diretto ai log: finché FDR scriveva
     * nell'execution log, quale pod avesse una giornata era leggibile lì, e togliendolo senza
     * questa colonna una giornata bloccata non sarebbe attribuibile ad alcun processo.
     */
    @Test
    @DisplayName("il claim scrive l'identificativo del processo su entrambe le righe")
    void ilClaimScriveLIdentificativoDelProcesso() {
        givenLockedPair(FdrProcessingStatus.PENDING, 0, FdrProcessingStatus.PENDING, 0);
        givenUpdateTouches(2);

        ownership.claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PENDING), NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).update(sql.capture(), args.capture(), args.capture(), args.capture(),
                args.capture(), args.capture(), args.capture(), args.capture(), args.capture(),
                args.capture());

        assertThat(sql.getValue()).contains("PROCESSING_INSTANCE_ID = ?");
        assertThat(args.getAllValues())
                .as("una sola UPDATE per entrambe le righe: lo stesso valore per costruzione")
                .contains(it.pagopa.cruscotto.ingestion.fdr.config.FdrInstanceId.get());
    }

    /**
     * Il rinnovo <strong>non</strong> tocca l'identificativo del processo né il token.
     *
     * <p>Riscriverlo a ogni battito sarebbe inutile nel caso normale — è lo stesso pod — e
     * dannoso nel caso che conta: se due worker si contendessero la giornata, l'ultimo a battere
     * sovrascriverebbe il nome dell'altro e la colonna smetterebbe di dire chi la possiede davvero.
     */
    @Test
    @DisplayName("il battito non cambia processo ne' token")
    void ilBattitoNonCambiaProcessoNeToken() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any()))
                .thenReturn(2);

        ownership.renew(someLease(), NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any(), any(), any(), any());
        assertThat(sql.getValue())
                .as("nel SET devono comparire solo lease e updated_at")
                .doesNotContain("PROCESSING_INSTANCE_ID =")
                .doesNotContain("PROCESSING_TOKEN =, ")
                .contains("SET LEASE_EXPIRES_AT = ?, UPDATED_AT = ?");
    }

    /**
     * Il takeover riscrive tutto: token nuovo, lease nuovo, processo nuovo, tentativo nuovo.
     *
     * <p>Il vecchio identificativo non deve sopravvivere su nessuna delle due righe, altrimenti la
     * giornata risulterebbe di un pod che non la sta più lavorando — che è il caso in cui qualcuno
     * andrebbe a cercare log inesistenti.
     */
    @Test
    @DisplayName("il takeover riscrive token, lease, processo e tentativo su entrambe")
    void ilTakeoverRiscriveTuttoSuEntrambe() {
        Instant scaduto = NOW.minusSeconds(1);
        givenLocked(List.<Object[]>of(
                row(1L, FdrDataset.FLUSSI_RENDICONTAZIONE, FdrProcessingStatus.PROCESSING, 2,
                        scaduto),
                row(2L, FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.PROCESSING, 2, scaduto)));
        givenUpdateTouches(2);

        FdrDayLease lease = ownership
                .claim(BATCH, DAY, EnumSet.of(FdrProcessingStatus.PROCESSING), NOW)
                .orElseThrow();

        assertThat(lease.attemptNo()).isEqualTo(3);
        assertThat(lease.leaseExpiresAt()).isEqualTo(NOW.plus(Duration.ofMinutes(5)));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any(), any(), any(), any(), any(),
                any(), any());
        assertThat(sql.getValue())
                .as("una sola UPDATE, senza filtro sul dataset: il vecchio processo non puo' "
                        + "sopravvivere su una delle due righe")
                .contains("PROCESSING_TOKEN = ?")
                .contains("LEASE_EXPIRES_AT = ?")
                .contains("PROCESSING_INSTANCE_ID = ?")
                .contains("ATTEMPT_COUNT = ?")
                .doesNotContain("DATASET = ?");
    }

    // ─── Recupero ────────────────────────────────────────────────────────────────
    //
    // La QUERY che trova le giornate abbandonate sta in FdrRetryService, accanto a quella delle
    // giornate da ritentare: e' il poller a usarle entrambe, con lo stesso codice. Il suo test e'
    // percio' in FdrRetryServiceTest. Qui resta cio' che riguarda il possesso - chi ce l'ha e come
    // lo si dimostra - non chi va a cercare le giornate a cui manca.

    // ─── Impalcatura ─────────────────────────────────────────────────────────────

    private FdrDayLease someLease() {
        return new FdrDayLease(BATCH, DAY, "token-1", 1,
                java.util.Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, 1L,
                        FdrDataset.IUV_RENDICONTATI, 2L),
                java.util.Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, FdrProcessingStatus.PENDING,
                        FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.PENDING),
                NOW.plusSeconds(300));
    }

    private void givenLockedPair(FdrProcessingStatus flussi, int flussiAttempts,
                                 FdrProcessingStatus iuv, int iuvAttempts) {
        givenLocked(List.<Object[]>of(
                row(1L, FdrDataset.FLUSSI_RENDICONTAZIONE, flussi, flussiAttempts, null),
                row(2L, FdrDataset.IUV_RENDICONTATI, iuv, iuvAttempts, null)));
    }

    /**
     * Le righe che il {@code SELECT ... FOR UPDATE} restituisce.
     *
     * <p>Si passa per il {@link RowMapper} vero invece di restituire oggetti già pronti: così il
     * test esercita anche la lettura delle colonne, che è il punto in cui un rinomino di colonna
     * passerebbe altrimenti inosservato fino al primo avvio.
     */
    @SuppressWarnings("unchecked")
    private void givenLocked(List<Object[]> rows) {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any()))
                .thenAnswer(invocation -> {
                    RowMapper<Object> mapper = invocation.getArgument(1);
                    List<Object> mapped = new ArrayList<>();
                    for (Object[] values : rows) {
                        mapped.add(mapper.mapRow(resultSetOf(values), 0));
                    }
                    return mapped;
                });
    }

    private void givenUpdateTouches(int rows) {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any(), any(), any(),
                any(), any())).thenReturn(rows);
    }

    private static Object[] row(long id, FdrDataset dataset, FdrProcessingStatus status,
                                int attempts, Instant leaseExpiresAt) {
        return new Object[] {id, dataset, status, attempts, leaseExpiresAt};
    }

    private static java.sql.ResultSet resultSetOf(Object[] values) throws java.sql.SQLException {
        java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
        when(rs.getLong("ID")).thenReturn((Long) values[0]);
        when(rs.getString("DATASET")).thenReturn(((FdrDataset) values[1]).name());
        when(rs.getString("STATUS")).thenReturn(((FdrProcessingStatus) values[2]).name());
        when(rs.getInt("ATTEMPT_COUNT")).thenReturn((Integer) values[3]);
        Instant lease = (Instant) values[4];
        when(rs.getTimestamp("LEASE_EXPIRES_AT"))
                .thenReturn(lease == null ? null : Timestamp.from(lease));
        return rs;
    }
}
