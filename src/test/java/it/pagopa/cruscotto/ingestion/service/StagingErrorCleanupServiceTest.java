package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Copertura della retention su {@code STG_INGEST_ERROR}.
 *
 * <p>Questo servizio e' rimasto rotto in produzione per giorni senza che si notasse: la DELETE unica
 * sfondava il {@code socketTimeout} del client e non cancellava nulla, ogni notte. I test qui bloccano
 * le tre proprieta' che impediscono che succeda di nuovo: cancellazione a batch (progresso acquisito),
 * tetti applicati lato server, e tetto di durata complessiva.</p>
 */
class StagingErrorCleanupServiceTest {

    private static final int BATCH = 1000;

    private JdbcTemplate jdbcTemplate;
    private IngestionConfig ingestionConfig;
    private StagingErrorCleanupService service;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        DbSchemaConfig schemaConfig = new DbSchemaConfig();
        schemaConfig.setSchema("ingestor");
        ingestionConfig = new IngestionConfig();
        ingestionConfig.getStagingErrorCleanup().setBatchSize(BATCH);
        ingestionConfig.getStagingErrorCleanup().setMaxDuration(Duration.ZERO);

        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());

        service = new StagingErrorCleanupService(jdbcTemplate, schemaConfig, ingestionConfig, transactionManager);
    }

    private void givenBatchResults(Integer first, Integer... rest) {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(first, rest);
    }

    /** La tabella e' partizionata (migration 47) oppure no (deploy del codice prima della migration). */
    private void givenPartitioned(boolean partitioned) {
        when(jdbcTemplate.queryForObject(anyString(), eq(Boolean.class), any(Object[].class)))
            .thenReturn(partitioned);
    }

    private void givenExpiredPartitions(String... names) {
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), any(Object[].class)))
            .thenReturn(List.of(names));
    }

    /** Conteggi usati prima di svuotare: righe totali e righe non riconciliate. */
    private void givenPartitionCounts(long total, long notDone) {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class)))
            .thenAnswer(invocation -> {
                String sql = invocation.getArgument(0);
                if (sql.contains("STATUS <> 'DONE'")) {
                    return notDone;
                }
                return total;
            });
    }

    /**
     * Il loop continua finche' un batch torna pieno e si ferma al primo batch parziale: e' il segnale
     * che non c'e' piu' nulla oltre la soglia.
     */
    @Test
    void deletesInBatchesUntilAPartialBatchSignalsTheEnd() {
        givenBatchResults(BATCH, BATCH, 120);

        long deleted = service.cleanup("run-1");

        assertThat(deleted).isEqualTo(2120L);
        verify(jdbcTemplate, times(3)).update(anyString(), any(Object[].class));
    }

    @Test
    void aSingleIncompleteBatchEndsTheLoopImmediately() {
        givenBatchResults(7);

        assertThat(service.cleanup("run-1")).isEqualTo(7L);
        verify(jdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }

    /**
     * I tetti devono essere applicati <strong>lato server</strong> su ogni batch: e' cio' che mancava.
     * Il solo {@code socketTimeout} del client non cancella la query, la lascia orfana e abortisce la
     * transazione senza cancellare nulla.
     */
    @Test
    void appliesServerSideStatementAndLockTimeoutOnEveryBatch() {
        givenBatchResults(BATCH, 5);

        service.cleanup("run-1");

        ArgumentCaptor<String> sessionSql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(4)).execute(sessionSql.capture());
        List<String> issued = sessionSql.getAllValues();
        assertThat(issued).filteredOn(s -> s.contains("statement_timeout")).hasSize(2);
        assertThat(issued).filteredOn(s -> s.contains("lock_timeout")).hasSize(2);
        // SET LOCAL, non SET: vale per la transazione e non inquina la connessione del pool.
        assertThat(issued).allMatch(s -> s.startsWith("SET LOCAL "));
    }

    /**
     * La selezione ordina per {@code CREATED_AT} e non per {@code ID}: e' quello che consente di
     * percorrere l'indice IDX_STG_INGEST_ERROR_CREATED_AT (migration 46) invece di ordinare a ogni
     * batch l'intero insieme selezionato.
     */
    @Test
    void theBatchSelectionWalksTheCreatedAtIndex() {
        givenBatchResults(3);

        service.cleanup("run-1");

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(Object[].class));
        assertThat(sql.getValue()).contains("WHERE CREATED_AT < ?");
        assertThat(sql.getValue()).contains("ORDER BY CREATED_AT");
        assertThat(sql.getValue()).doesNotContain("ORDER BY ID");
    }

    @Test
    void doesNothingWhenDisabled() {
        ingestionConfig.getStagingErrorCleanup().setEnabled(false);

        assertThat(service.cleanup("run-1")).isZero();
        verify(jdbcTemplate, never()).update(anyString(), any(Object[].class));
    }

    /**
     * Con un arretrato di giorni la prima esecuzione riuscita avrebbe moltissimo da cancellare: il
     * tetto di durata la interrompe, e il progresso resta perche' ogni batch e' gia' committato.
     */
    @Test
    void stopsAtTheDurationBudgetLeavingTheRestToTheNextRun() {
        ingestionConfig.getStagingErrorCleanup().setMaxDuration(Duration.ofMillis(1));
        // Ogni batch torna pieno: senza il tetto il loop non terminerebbe mai.
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenAnswer(invocation -> {
            Thread.sleep(5);
            return BATCH;
        });

        long deleted = service.cleanup("run-1");

        assertThat(deleted).isEqualTo(BATCH);
        verify(jdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }

    @Test
    void aZeroBudgetMeansNoTimeLimit() {
        ingestionConfig.getStagingErrorCleanup().setMaxDuration(Duration.ZERO);
        givenBatchResults(BATCH, BATCH, 1);

        assertThat(service.cleanup("run-1")).isEqualTo(2001L);
        verify(jdbcTemplate, times(3)).update(anyString(), any(Object[].class));
    }

    // ── Tabella partizionata per giorno (migration 47): retention via TRUNCATE ──────────────────

    /**
     * Su tabella partizionata la retention svuota le partizioni scadute: operazione di metadati,
     * senza cancellazione riga per riga, senza bloat e senza manutenzione degli indici.
     */
    @Test
    void truncatesExpiredPartitionsInsteadOfDeletingRows() {
        givenPartitioned(true);
        givenExpiredPartitions("stg_ingest_error_20260901", "stg_ingest_error_20260902");
        givenPartitionCounts(120, 0);

        long removed = service.cleanup("run-1");

        assertThat(removed).isEqualTo(240L);
        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, atLeastOnce()).execute(statements.capture());
        assertThat(statements.getAllValues())
            .contains("TRUNCATE TABLE ingestor.stg_ingest_error_20260901",
                      "TRUNCATE TABLE ingestor.stg_ingest_error_20260902");
        // Nessuna DELETE: e' l'intero punto del partizionamento.
        verify(jdbcTemplate, never()).update(anyString(), any(Object[].class));
    }

    /** Una partizione gia' vuota non viene toccata: niente lock, niente log inutile. */
    @Test
    void skipsPartitionsThatAreAlreadyEmpty() {
        givenPartitioned(true);
        givenExpiredPartitions("stg_ingest_error_20260901");
        givenPartitionCounts(0, 0);

        assertThat(service.cleanup("run-1")).isZero();
        // Nessuno statement emesso: ne' il TRUNCATE ne' il SET LOCAL che lo precede. Una partizione
        // vuota non vale un lock.
        verify(jdbcTemplate, never()).execute(anyString());
    }

    /**
     * Prima di svuotare si contano i record non riconciliati: sono dati ADX che la retention sta per
     * eliminare senza che siano stati recuperati. Senza questo conteggio la perdita sarebbe
     * silenziosa, e con il TRUNCATE spariscono un giorno intero alla volta.
     */
    @Test
    void accountsForRecordsLostBeyondRetentionBeforeTruncating() {
        givenPartitioned(true);
        givenExpiredPartitions("stg_ingest_error_20260901");
        givenPartitionCounts(50, 7);

        service.cleanup("run-1");

        ArgumentCaptor<String> counts = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, atLeastOnce()).queryForObject(counts.capture(), eq(Long.class));
        assertThat(counts.getAllValues()).anyMatch(s -> s.contains("STATUS <> 'DONE'"));
    }

    /**
     * Il codice non assume che la migration sia passata: se la tabella non e' ancora partizionata
     * (deploy del codice prima della migration, o ambiente locale non aggiornato) la retention resta
     * la DELETE a batch, che e' corretta. Evita che un ordine di deploy sfortunato fermi la retention.
     */
    @Test
    void fallsBackToBatchedDeleteWhenTheTableIsNotPartitionedYet() {
        givenPartitioned(false);
        givenBatchResults(3);

        assertThat(service.cleanup("run-1")).isEqualTo(3L);
        verify(jdbcTemplate, times(1)).update(anyString(), any(Object[].class));
    }
}
