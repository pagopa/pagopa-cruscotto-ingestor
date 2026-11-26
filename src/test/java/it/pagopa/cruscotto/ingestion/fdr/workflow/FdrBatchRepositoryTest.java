package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * La tabella delle richieste, e soprattutto il claim.
 *
 * <p>Il claim e' la riga che rende sicuro il deploy a piu' pod, e come tutte le proprieta' di
 * concorrenza non si vede mai in sviluppo: con un pod solo funziona identica anche se e' sbagliata.
 * Si manifesta in produzione, dove i pod sono tre, e si manifesta come <em>doppia esecuzione</em> —
 * per una REINGESTION, due {@code TRUNCATE} intrecciati alle rispettive riscritture.
 *
 * <p>I servizi che la usano la mockano, quindi se la garanzia non e' verificata qui non lo e' da
 * nessuna parte.
 */
class FdrBatchRepositoryTest {

    private static final Instant NOW = Instant.parse("2026-11-18T10:00:00Z");
    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final FdrBatchRepository repository = newRepository();

    private FdrBatchRepository newRepository() {
        DbSchemaConfig schema = new DbSchemaConfig();
        schema.setSchema("sert_ingestor");
        return new FdrBatchRepository(jdbcTemplate, schema);
    }

    // ─── Claim ───────────────────────────────────────────────────────────────────

    /**
     * La transizione e' una sola {@code UPDATE} condizionata sullo stato precedente.
     *
     * <p>PostgreSQL blocca la riga per la durata dello statement, quindi fra due pod che tentano
     * insieme uno solo vede una riga aggiornata. Senza la condizione su {@code PENDING} entrambi
     * aggiornerebbero, entrambi vedrebbero successo, ed entrambi lavorerebbero.
     */
    @Test
    void ilClaimEunaUpdateCondizionataSulloStatoPrecedente() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any())).thenReturn(1);

        repository.claim(7L, NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).update(sql.capture(), args.capture(), args.capture(), args.capture(),
                args.capture(), args.capture());

        assertThat(sql.getValue())
                .as("senza la condizione sullo stato il claim non e' atomico")
                .contains("WHERE ID = ? AND STATUS = ?");
        assertThat(args.getAllValues())
                .containsSubsequence(7L, FdrBatchRepository.PENDING);
    }

    /** Zero righe aggiornate significa che un altro pod e' arrivato prima: il claim e' perso. */
    @Test
    void zeroRigheAggiornateSignificaClaimPerso() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any())).thenReturn(0);

        assertThat(repository.claim(7L, NOW)).isFalse();
    }

    @Test
    void unaRigaAggiornataSignificaClaimVinto() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any(), any())).thenReturn(1);

        assertThat(repository.claim(7L, NOW)).isTrue();
    }

    /**
     * Anche il rilascio e' condizionato, e per la stessa ragione.
     *
     * <p>Fra due pod che decidono insieme che una richiesta e' abbandonata, uno solo la rimette in
     * coda: altrimenti due eventi di recupero per la stessa richiesta, e il rischio di rilasciarne
     * una che nel frattempo qualcuno ha ripreso.
     */
    @Test
    void ilRilascioEcondizionatoSulloStatoInLavorazione() {
        when(jdbcTemplate.update(anyString(), any(), any(), any(), any())).thenReturn(1);

        assertThat(repository.releaseToPending(7L, NOW)).isTrue();

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).update(sql.capture(), args.capture(), args.capture(), args.capture(),
                args.capture());

        assertThat(sql.getValue()).contains("WHERE ID = ? AND STATUS = ?");
        assertThat(args.getAllValues())
                .containsSubsequence(FdrBatchRepository.PENDING, 7L,
                        FdrProcessingStatus.PROCESSING.name());
    }

    // ─── Selezione ───────────────────────────────────────────────────────────────

    /**
     * Il polling guarda solo le richieste amministrative, mai il DAILY.
     *
     * <p>Il DAILY se lo crea lo scheduler e nasce gia' in lavorazione proprio per non finire qui:
     * se il poller potesse prenderlo, la stessa giornata verrebbe elaborata due volte, e la
     * scrittura e' un INSERT fedele senza deduplica.
     */
    @Test
    void ilPollingPrendeSoloBootstrapEReingestion() {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any()))
                .thenReturn(List.of());

        repository.findPendingCommands();

        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).query(anyString(), any(RowMapper.class), args.capture(),
                args.capture(), args.capture());

        assertThat(args.getAllValues()).containsExactly(FdrBatchRepository.PENDING,
                FdrWorkflow.BOOTSTRAP.name(), FdrWorkflow.REINGESTION.name());
    }

    /** Le giornate vengono dagli status, in ordine, e distinte. */
    @Test
    void leGiornateVengonoDagliStatusInOrdine() {
        when(jdbcTemplate.queryForList(anyString(), eq(Date.class), any()))
                .thenReturn(List.of(Date.valueOf(DAY)));

        assertThat(repository.findBusinessDays(7L)).containsExactly(DAY);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).queryForList(sql.capture(), eq(Date.class), any());
        assertThat(sql.getValue())
                .contains("FDR_INGESTION_STATUS")
                .contains("DISTINCT BUSINESS_DAY")
                .contains("ORDER BY BUSINESS_DAY")
                .as("il perimetro non viene dagli estremi della richiesta")
                .doesNotContain("REQUESTED_FROM");
    }

    // ─── Aggregazione ────────────────────────────────────────────────────────────

    /**
     * L'esito del batch, e la precedenza fra gli stati.
     *
     * <p>Gli stati non terminali vincono su tutto: finche' una giornata e' in lavorazione o attende
     * un tentativo, il batch <em>non ha ancora</em> un esito, e scriverne uno sarebbe inventarselo.
     * Poi vengono i negativi terminali, e solo se nulla e' andato storto si guarda ai warning.
     */
    @Test
    void lEsitoDelBatchSegueLaPrecedenzaFraGliStati() {
        assertThat(FdrBatchRepository.aggregate(List.of(FdrProcessingStatus.COMPLETED,
                FdrProcessingStatus.COMPLETED))).isEqualTo(FdrProcessingStatus.COMPLETED);

        assertThat(FdrBatchRepository.aggregate(List.of(FdrProcessingStatus.COMPLETED,
                FdrProcessingStatus.COMPLETED_WITH_WARNING)))
                .isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);

        assertThat(FdrBatchRepository.aggregate(List.of(FdrProcessingStatus.COMPLETED,
                FdrProcessingStatus.MISSING)))
                .as("una giornata senza sorgente pesa come una fallita: meta' del dato non c'e'")
                .isEqualTo(FdrProcessingStatus.FAILED);

        assertThat(FdrBatchRepository.aggregate(List.of(FdrProcessingStatus.FAILED,
                FdrProcessingStatus.WAITING_RETRY)))
                .as("con un tentativo ancora in sospeso il batch non e' terminale")
                .isEqualTo(FdrProcessingStatus.WAITING_RETRY);

        assertThat(FdrBatchRepository.aggregate(List.of(FdrProcessingStatus.WAITING_RETRY,
                FdrProcessingStatus.PROCESSING)))
                .isEqualTo(FdrProcessingStatus.PROCESSING);
    }

    /**
     * Uno stato che l'enum non conosce <strong>non</strong> rende il batch completo.
     *
     * <p>La colonna non ha un CHECK, quindi il database accetterebbe qualunque stringa. Scartare
     * silenziosamente i valori estranei — come faceva la prima stesura — faceva sparire
     * dall'aggregazione proprio le righe di cui non si sa nulla: un batch con una giornata in uno
     * stato ignoto risultava COMPLETED, cioe' consultabile dal backend.
     *
     * <p>La lettura prudente e' "non terminale": in quello stato nessun percorso distruttivo parte
     * e il backend non espone la giornata. L'anomalia viene comunque segnalata a ERROR.
     */
    @Test
    void unoStatoSconosciutoNonRendeIlBatchCompleto() {
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), any()))
                .thenReturn(List.of("COMPLETED", "QUALCOSA_DI_IGNOTO"));

        List<FdrProcessingStatus> statuses = repository.findStatuses(7L);

        assertThat(statuses)
                .as("il valore ignoto resta nella lista, non viene scartato")
                .hasSize(2)
                .containsNull();
        assertThat(FdrBatchRepository.aggregate(statuses))
                .as("un batch con uno stato ignoto non e' completo")
                .isEqualTo(FdrProcessingStatus.PROCESSING);
    }

    /**
     * Una giornata registrata e mai presa pesa quanto una in lavorazione.
     *
     * <p>E' il caso di una richiesta amministrativa in cui una giornata e' stata respinta e non ha
     * mai ricevuto un esito: senza questa regola il batch si chiuderebbe come completo mentre una
     * delle giornate chieste non e' mai stata fatta.
     */
    @Test
    void unaGiornataInAttesaNonChiudeIlBatch() {
        assertThat(FdrBatchRepository.aggregate(
                List.of(FdrProcessingStatus.COMPLETED, FdrProcessingStatus.PENDING)))
                .isEqualTo(FdrProcessingStatus.PROCESSING);
    }

    /**
     * {@code PENDING} non appartiene all'enum degli esiti, ed e' uno stato legittimo di una riga di
     * status: una giornata registrata e non ancora presa. Letta come {@code null}, pesa come "non
     * terminale" — che e' esattamente cio' che e'.
     */
    @Test
    void unaGiornataNonAncoraPresaRendeIlBatchNonTerminale() {
        List<FdrProcessingStatus> withPending = new java.util.ArrayList<>();
        withPending.add(FdrProcessingStatus.COMPLETED);
        withPending.add(null);

        assertThat(FdrBatchRepository.aggregate(withPending))
                .isEqualTo(FdrProcessingStatus.PROCESSING);
    }
}
