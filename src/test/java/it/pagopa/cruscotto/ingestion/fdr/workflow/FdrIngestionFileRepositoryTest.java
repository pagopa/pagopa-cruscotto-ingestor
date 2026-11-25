package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Il registro di cosa è successo a ogni file.
 *
 * <p>Due garanzie sopra le altre, perché sono quelle che si perdono senza accorgersene: la chiusura
 * deve essere <strong>un solo</strong> UPDATE — due statement significano che il secondo può non
 * essere mai eseguito, lasciando una riga chiusa senza numeri — e un guasto non deve azzerare i
 * contatori parziali, che su un file rotto a metà sono l'unica informazione per cui lo si guarda.
 */
class FdrIngestionFileRepositoryTest {

    private static final long BATCH = 7L;
    private static final long STATUS = 42L;
    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final Instant NOW = Instant.parse("2026-11-25T10:00:00Z");

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final FdrIngestionFileRepository repository = newRepository();

    private FdrIngestionFileRepository newRepository() {
        DbSchemaConfig schema = new DbSchemaConfig();
        schema.setSchema("sert_ingestor");
        return new FdrIngestionFileRepository(jdbcTemplate, schema);
    }

    // ─── Apertura ────────────────────────────────────────────────────────────────

    /**
     * La riga nasce in lavorazione, con i metadata del blob e i contatori azzerati.
     *
     * <p>{@code ROWS_SOURCE} resta invece <strong>nullo</strong>: a file non ancora letto il suo
     * totale non esiste, e scriverci zero direbbe "file vuoto".
     */
    @Test
    void laRigaNasceInLavorazioneConIMetadataDelBlob() {
        givenInsertReturns(1L);

        long id = repository.begin(BATCH, STATUS, file(10, 2048, "etag-7"), 1, NOW);

        assertThat(id).isEqualTo(1L);
        String sql = capturedInsertSql();
        assertThat(sql).contains("INSERT INTO sert_ingestor.FDR_INGESTION_FILE");
        assertThat(sql)
                .as("i contatori partono da zero e il totale resta ignoto")
                .contains("NULL, 0, 0, 0, 0");
        assertThat(capturedInsertArgs()).contains(FdrProcessingStatus.PROCESSING.name());
    }

    @Test
    void iMetadataVengonoTuttiDalBlobDellaDiscovery() {
        givenInsertReturns(1L);

        repository.begin(BATCH, STATUS, file(10, 2048, "etag-7"), 3, NOW);

        assertThat(capturedInsertArgs()).contains(
                BATCH,
                STATUS,
                java.sql.Date.valueOf(DAY),
                FdrDataset.IUV_RENDICONTATI.name(),
                "20260915/IUV_RENDICONTATI_20260915-10.csv.gz",
                BigDecimal.TEN,
                2048L,
                "etag-7",
                3);
    }

    /**
     * Il dataset viene dal blob, non da una seconda lettura del nome del file.
     *
     * <p>La discovery lo ha già determinato: ricavarlo di nuovo qui significherebbe avere due
     * regole per la stessa cosa, e due regole prima o poi divergono.
     */
    @Test
    void ilDatasetNonVieneDedottoDalNomeDelFile() {
        givenInsertReturns(1L);

        FdrBlobFile mislabelled = new FdrBlobFile(
                "20260915/IUV_RENDICONTATI_20260915-1.csv.gz",
                FdrDataset.FLUSSI_RENDICONTAZIONE, DAY, BigInteger.ONE, 1, "e", null);
        repository.begin(BATCH, STATUS, mislabelled, 1, NOW);

        assertThat(capturedInsertArgs())
                .as("il valore e' quello del blob, non quello suggerito dal nome")
                .contains(FdrDataset.FLUSSI_RENDICONTAZIONE.name())
                .doesNotContain(FdrDataset.IUV_RENDICONTATI.name());
    }

    /**
     * Un progressivo più grande di un intero non va perso.
     *
     * <p>Il parser del nome blob usa {@code BigInteger} perché il cliente non ha posto un limite al
     * numero di cifre. Se qui si stringesse a un intero, il file più anomalo — cioè quello per cui
     * il log serve — sarebbe l'unico a non poter essere registrato.
     */
    @Test
    void unProgressivoOltreIlLimiteDellInteroNonVienePerso() {
        givenInsertReturns(1L);
        BigInteger huge = new BigInteger("99999999999999999999");

        FdrBlobFile file = new FdrBlobFile("20260915/IUV_RENDICONTATI_20260915-" + huge + ".csv.gz",
                FdrDataset.IUV_RENDICONTATI, DAY, huge, 1, "e", null);
        repository.begin(BATCH, STATUS, file, 1, NOW);

        assertThat(capturedInsertArgs()).contains(new BigDecimal(huge));
    }

    /**
     * Dimensione ignota significa {@code NULL}, mai {@code -1}.
     *
     * <p>Azure usa {@code -1} per dire "non lo so", ma in colonna sarebbe un numero come gli altri:
     * una somma lo sommerebbe, un confronto lo userebbe. Il database ha già il modo di dire che un
     * dato non c'è.
     */
    @Test
    void unaDimensioneIgnotaDiventaNullENonMenoUno() {
        givenInsertReturns(1L);

        repository.begin(BATCH, STATUS, file(1, -1, "etag"), 1, NOW);

        assertThat(capturedInsertArgs()).doesNotContain(-1L);
    }

    /** Nessun segreto in colonna: il blob si identifica col suo path, non con un URL firmato. */
    @Test
    void nessunSegretoAzureFinisceInColonna() {
        givenInsertReturns(1L);

        repository.begin(BATCH, STATUS, file(1, 1, "etag"), 1, NOW);

        String sql = capturedInsertSql().toLowerCase(java.util.Locale.ROOT);
        assertThat(sql).doesNotContain("sas").doesNotContain("sig=")
                .doesNotContain("accountkey").doesNotContain("https://");
        capturedInsertArgs().stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .forEach(value -> assertThat(value.toLowerCase(java.util.Locale.ROOT))
                        .as("valore sospetto in colonna: %s", value)
                        .doesNotContain("https://").doesNotContain("sig=")
                        .doesNotContain("accountkey"));
    }

    // ─── Chiusura ────────────────────────────────────────────────────────────────

    /**
     * Un solo UPDATE chiude la riga, con stato, contatori, fine e durata insieme.
     *
     * <p>Separarli in due statement vorrebbe dire che il secondo può non essere eseguito, e la riga
     * resterebbe chiusa senza numeri o con numeri senza esito — proprio nei casi che interessa
     * analizzare, perché è quando qualcosa va storto che il secondo statement salta.
     */
    @Test
    void laChiusuraEunSoloUpdate() {
        repository.complete(1L, FdrProcessingStatus.COMPLETED,
                new FdrFileCounters(100L, 100, 100, 0, 0), NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(1)).update(sql.capture(), any(Object[].class), any(int[].class));
        assertThat(sql.getValue())
                .contains("SET STATUS = ?")
                .contains("ROWS_SOURCE = ?")
                .contains("COMPLETED_AT = ?")
                .contains("DURATION_MS =")
                .contains("WHERE ID = ?");
    }

    /**
     * La durata la calcola il database, non il pod.
     *
     * <p>Quartz è clusterizzato: l'apertura e la chiusura di un file possono avvenire su pod
     * diversi, e due orologi che differiscono di qualche secondo produrrebbero una durata negativa
     * — un valore che non si spiega a nessuno. {@code STARTED_AT} è già in riga: il conto si fa lì.
     */
    @Test
    void laDurataVieneCalcolataDalDatabaseDaStartedAt() {
        repository.complete(1L, FdrProcessingStatus.COMPLETED,
                new FdrFileCounters(1L, 1, 1, 0, 0), NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(Object[].class), any(int[].class));
        assertThat(sql.getValue()).contains("EXTRACT(EPOCH FROM (? - STARTED_AT))");
    }

    @Test
    void unaChiusuraRiuscitaNonLasciaCodiciDiErrore() {
        repository.complete(1L, FdrProcessingStatus.COMPLETED_WITH_WARNING,
                new FdrFileCounters(100L, 100, 98, 2, 0), NOW);

        List<Object> args = capturedUpdateArgs();
        assertThat(args).contains(FdrProcessingStatus.COMPLETED_WITH_WARNING.name(), 100L, 98L, 2L);
        assertThat(args).as("nessun errore su un esito di successo").doesNotContain("null");
    }

    /**
     * Un guasto conserva i contatori parziali e lascia {@code ROWS_SOURCE} nullo.
     *
     * <p>È il caso per cui questa tabella esiste: un file da tre milioni di righe interrotto a
     * quarantamila deve risultare interrotto a quarantamila, non completato con quarantamila.
     */
    @Test
    void unGuastoConservaIContatoriParzialiESenzaTotale() {
        repository.fail(1L, new FdrFileCounters(null, 40_000, 39_998, 2, 0),
                "FdrPersistenceException", "connection reset", NOW);

        List<Object> args = capturedUpdateArgs();
        assertThat(args).contains(FdrProcessingStatus.FAILED.name(), 40_000L, 39_998L, 2L,
                "FdrPersistenceException", "connection reset");
        assertThat(args).as("il totale del file resta ignoto").contains((Object) null);
    }

    /** Un messaggio più lungo della colonna viene accorciato: è testo nostro, non del sorgente. */
    @Test
    void unMessaggioTroppoLungoVieneAccorciatoInveceDiFarFallireLUpdate() {
        repository.fail(1L, FdrFileCounters.empty(), "Kind", "x".repeat(5_000), NOW);

        boolean withinLimit = capturedUpdateArgs().stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .allMatch(value -> value.length() <= 1_000);
        assertThat(withinLimit).isTrue();
    }

    // ─── Progresso ───────────────────────────────────────────────────────────────

    /**
     * Il progresso non chiude niente: nessuna data di fine, nessuna durata, nessuno stato terminale.
     *
     * <p>Una riga che avesse già una data di fine e fosse ancora in lavorazione sarebbe la
     * contraddizione che questo log esiste per evitare.
     */
    @Test
    void ilProgressoNonChiudeLaRiga() {
        repository.progress(1L, new FdrFileCounters(null, 1_000, 1_000, 0, 0), NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(), any(), any(), any(), any(), any());
        assertThat(sql.getValue())
                .contains("ROWS_READ = ?")
                .doesNotContain("STATUS = ?")
                .doesNotContain("COMPLETED_AT")
                .doesNotContain("DURATION_MS");
    }

    // ─── Tentativi ───────────────────────────────────────────────────────────────

    /**
     * Due letture dello stesso blob nello stesso batch producono due righe, non un aggiornamento.
     *
     * <p>"Fallito al primo tentativo, riuscito al secondo" sono due fatti. Sovrascrivere il primo
     * cancellerebbe l'unica traccia del guasto, cioè proprio ciò che si va a cercare.
     */
    @Test
    void dueTentativiSulloStessoBlobProduconoDueRighe() {
        givenInsertReturns(1L, 2L);
        FdrBlobFile sameFile = file(10, 2048, "etag-7");

        long first = repository.begin(BATCH, STATUS, sameFile, 1, NOW);
        long second = repository.begin(BATCH, STATUS, sameFile, 2, NOW);

        assertThat(first).isNotEqualTo(second);
        verify(jdbcTemplate, times(2))
                .queryForObject(anyString(), any(Object[].class), any(int[].class), eq(Long.class));
        assertThat(capturedInsertSql())
                .as("due INSERT, nessun upsert: i tentativi non si fondono")
                .doesNotContain("ON CONFLICT");
    }

    // ─── Utilità ─────────────────────────────────────────────────────────────────

    private void givenInsertReturns(Long first, Long... rest) {
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), any(int[].class),
                eq(Long.class))).thenReturn(first, rest);
    }

    private String capturedInsertSql() {
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, org.mockito.Mockito.atLeastOnce())
                .queryForObject(sql.capture(), any(Object[].class), any(int[].class), eq(Long.class));
        return sql.getValue();
    }

    private List<Object> capturedInsertArgs() {
        ArgumentCaptor<Object[]> args = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate, org.mockito.Mockito.atLeastOnce())
                .queryForObject(anyString(), args.capture(), any(int[].class), eq(Long.class));
        return java.util.Arrays.asList(args.getValue());
    }

    private List<Object> capturedUpdateArgs() {
        ArgumentCaptor<Object[]> args = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate).update(anyString(), args.capture(), any(int[].class));
        return java.util.Arrays.asList(args.getValue());
    }

    private static FdrBlobFile file(int progressive, long sizeBytes, String eTag) {
        return new FdrBlobFile(
                "20260915/IUV_RENDICONTATI_20260915-" + progressive + ".csv.gz",
                FdrDataset.IUV_RENDICONTATI, DAY, BigInteger.valueOf(progressive), sizeBytes,
                eTag, null);
    }
}
