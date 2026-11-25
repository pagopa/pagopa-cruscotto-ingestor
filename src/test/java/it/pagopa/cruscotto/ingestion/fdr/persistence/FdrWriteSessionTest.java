package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFlussoRow;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * La sessione di scrittura: batch, fallback, troncamento, conteggi.
 *
 * <p>Il database e' finto ma il percorso e' quello vero: ogni batch passa per il
 * {@code TransactionTemplate} e per {@code batchUpdate}, e il finto database decide di fallire
 * <strong>guardando le righe effettivamente bindate</strong>. E' l'unico modo perche' questi test
 * verifichino il codice di produzione e non la propria impalcatura.
 *
 * <p>La domanda a cui rispondono quasi tutti: una riga rifiutata non deve costare le altre, e un
 * guasto del database non deve diventare una tempesta di tentativi.
 */
class FdrWriteSessionTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final int ID_FLUSSO_PARAMETER = 3;

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final PlatformTransactionManager transactionManager =
            mock(PlatformTransactionManager.class);
    private final TransactionTemplate transactionTemplate =
            new TransactionTemplate(transactionManager);

    /** Le righe che il finto database rifiuta, per ID_FLUSSO. */
    private final Set<String> rejected = new java.util.HashSet<>();
    /** Ogni tentativo di batch, con gli ID_FLUSSO che conteneva. */
    private final List<List<String>> attempts = new ArrayList<>();

    FdrWriteSessionTest() {
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
    }

    // ─── Happy path e batching ───────────────────────────────────────────────────

    @Test
    void ilPercorsoFeliceEUnSoloBatchSenzaFallback() {
        givenDatabase();
        FdrWriteSession session = session(1000);

        session.accept(rows(4));
        FdrWriteResult result = session.complete();

        assertThat(attempts).hasSize(1);
        assertThat(result.rowsReceived()).isEqualTo(4);
        assertThat(result.rowsInserted()).isEqualTo(4);
        assertThat(result.rowsRejected()).isZero();
        assertThat(result.batchesAttempted()).isEqualTo(1);
        assertThat(result.batchesSucceeded()).isEqualTo(1);
        assertThat(result.batchesSplit()).isZero();
        assertThat(result.isComplete()).isTrue();
    }

    @Test
    void unBatchVuotoONulloNonTocccaIlDatabase() {
        FdrWriteSession session = session(1000);

        session.accept(List.of());
        session.accept(null);

        verifyNoInteractions(jdbcTemplate);
        assertThat(session.complete().rowsReceived()).isZero();
    }

    @Test
    void unBatchEsattamenteAlLimiteRestaUnoSolo() {
        givenDatabase();
        FdrWriteSession session = session(2);

        session.accept(rows(2));

        assertThat(attempts).hasSize(1);
        assertThat(attempts.get(0)).hasSize(2);
    }

    /**
     * Una lista piu' grande del limite viene spezzata: il writer non si fida del fatto che il
     * lettore abbia gia' dimensionato i batch, altrimenti un chiamante diverso renderebbe illimitato
     * un batch JDBC. Il limite e' lo stesso, {@code fdr.processing.batch-size}: non ce n'e' un
     * secondo.
     */
    @Test
    void unaListaOltreIlLimiteVieneSpezzataInBatchDellaDimensioneConfigurata() {
        givenDatabase();
        FdrWriteSession session = session(2);

        session.accept(rows(5));

        assertThat(attempts).hasSize(3);
        assertThat(attempts.stream().map(List::size)).containsExactly(2, 2, 1);
    }

    // ─── Fallback: una riga rifiutata non costa le altre ─────────────────────────

    @Test
    void unaRigaRifiutataNonFaPerdereIlRestoDelBatch() {
        rejected.add("R5");
        givenDatabase();
        FdrWriteSession session = session(10);

        session.accept(rows(10));
        FdrWriteResult result = session.complete();

        assertThat(result.rowsInserted()).isEqualTo(9);
        assertThat(result.rowsRejected()).isEqualTo(1);
        assertThat(result.batchesSplit()).isPositive();
        assertThat(insertedRows()).hasSize(9).doesNotContain("R5");
    }

    @Test
    void laPrimaELUltimaRigaRifiutataSonoGestiteComeLeAltre() {
        rejected.addAll(Set.of("R0", "R9"));
        givenDatabase();
        FdrWriteSession session = session(10);

        session.accept(rows(10));
        FdrWriteResult result = session.complete();

        assertThat(result.rowsInserted()).isEqualTo(8);
        assertThat(result.rowsRejected()).isEqualTo(2);
        assertThat(insertedRows()).hasSize(8).doesNotContain("R0", "R9");
    }

    /**
     * Nessuna riga viene scritta due volte durante lo split. E' la garanzia che rende sicuro il
     * fallback: il template chiude la transazione fallita prima che le meta' partano, quindi le
     * righe del tentativo fallito non possono restare.
     */
    @Test
    void loSplitNonDuplicaMaiUnaRiga() {
        rejected.add("R3");
        givenDatabase();
        FdrWriteSession session = session(8);

        session.accept(rows(8));

        List<String> inserted = insertedRows();
        assertThat(inserted).doesNotHaveDuplicates().hasSize(7);
    }

    /**
     * Il batch interamente invalido e' il caso peggiore: 2n-1 tentativi, cioe' l'albero binario
     * completo. E' il prezzo per non scrivere nulla di sbagliato, ed e' visibile in
     * {@code batchesAttempted}.
     */
    @Test
    void unBatchInteramenteInvalidoCostaDueNMenoUnoTentativi() {
        rejected.addAll(Set.of("R0", "R1", "R2", "R3"));
        givenDatabase();
        FdrWriteSession session = session(4);

        session.accept(rows(4));
        FdrWriteResult result = session.complete();

        assertThat(result.rowsInserted()).isZero();
        assertThat(result.rowsRejected()).isEqualTo(4);
        assertThat(result.batchesAttempted()).isEqualTo(7);
        assertThat(result.batchesSucceeded()).isZero();
    }

    /**
     * Il costo del caso con una riga sola sbagliata e' logaritmico, non lineare: con 64 righe sono
     * una dozzina di tentativi, non 64 insert singoli. E' la ragione per cui il fallback divide
     * invece di scendere subito alla riga.
     */
    @Test
    void unaSolaRigaInvalidaCostaUnNumeroLogaritmicoDiTentativi() {
        rejected.add("R42");
        givenDatabase();
        FdrWriteSession session = session(64);

        session.accept(rows(64));
        FdrWriteResult result = session.complete();

        assertThat(result.rowsInserted()).isEqualTo(63);
        assertThat(result.batchesAttempted()).isLessThan(20);
    }

    @Test
    void lErroreDiRigaRiportaLaPosizioneELoSqlstate() {
        rejected.add("R2");
        givenDatabase();
        FdrWriteSession session = session(4);

        session.accept(rows(4));
        FdrWriteResult result = session.complete();

        assertThat(result.errorSample()).singleElement().satisfies(error -> {
            assertThat(error.rowOrdinal()).isEqualTo(3);
            assertThat(error.sqlState()).isEqualTo("22001");
            assertThat(error.category()).isEqualTo(FdrPersistenceError.Category.DATA_ERROR);
            assertThat(error.dataset()).isEqualTo(FdrDataset.FLUSSI_RENDICONTAZIONE);
            assertThat(error.businessDay()).isEqualTo(DAY);
            assertThat(error.message()).contains("value too long");
        });
    }

    @Test
    void ilCampioneDegliErroriELimitatoMaIlConteggioNo() {
        IntStream.range(0, 30).forEach(i -> rejected.add("R" + i));
        givenDatabase();
        FdrWriteSession session = session(32);

        session.accept(rows(30));
        FdrWriteResult result = session.complete();

        assertThat(result.rowsRejected()).isEqualTo(30);
        assertThat(result.errorSample()).hasSize(FdrWriteSession.MAX_ERROR_SAMPLES);
    }

    // ─── Guasto tecnico: nessuno split ───────────────────────────────────────────

    /**
     * Il test che vale piu' di tutti. Durante un disservizio del database uno split ricorsivo
     * produrrebbe migliaia di tentativi inutili contro un database gia' in difficolta', e
     * seppellirebbe la causa vera sotto di essi. L'errore deve uscire al primo colpo.
     */
    @Test
    void unGuastoTecnicoNonVieneMaiDiviso() {
        RuntimeException outage = new DataAccessResourceFailureException("connection reset",
                new SQLException("closed", "08006"));
        when(jdbcTemplate.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenThrow(outage);
        FdrWriteSession session = session(64);

        assertThatThrownBy(() -> session.accept(rows(64))).isSameAs(outage);

        verify(jdbcTemplate, times(1))
                .batchUpdate(anyString(), any(BatchPreparedStatementSetter.class));
        assertThat(session.complete().batchesSplit()).isZero();
    }

    /** Un guasto senza SQLSTATE non e' dimostrabilmente isolabile, quindi e' tecnico. */
    @Test
    void unGuastoSenzaSqlstateNonVieneDiviso() {
        RuntimeException unknown = new IllegalStateException("pool exhausted");
        when(jdbcTemplate.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenThrow(unknown);
        FdrWriteSession session = session(16);

        assertThatThrownBy(() -> session.accept(rows(16))).isSameAs(unknown);
        verify(jdbcTemplate, times(1))
                .batchUpdate(anyString(), any(BatchPreparedStatementSetter.class));
    }

    // ─── Transazioni ─────────────────────────────────────────────────────────────

    /**
     * Il batch fallito viene chiuso prima che le meta' partano. Se non lo fosse, i sotto-batch
     * girerebbero dentro una transazione gia' marcata rollback-only e il fallback scriverebbe
     * zero righe credendo di averle scritte.
     */
    @Test
    void ilBatchFallitoVieneChiusoPrimaDelloSplit() {
        rejected.add("R1");
        givenDatabase();
        FdrWriteSession session = session(4);

        session.accept(rows(4));

        InOrder order = inOrder(transactionManager);
        order.verify(transactionManager).getTransaction(any());
        order.verify(transactionManager).rollback(any());
        order.verify(transactionManager).getTransaction(any());
    }

    @Test
    void ogniBatchRiuscitoVieneCommessoUnaVoltaSola() {
        givenDatabase();
        FdrWriteSession session = session(2);

        session.accept(rows(4));

        verify(transactionManager, times(2)).commit(any());
        verify(transactionManager, never()).rollback(any());
    }

    // ─── Troncamento della causale ───────────────────────────────────────────────

    /**
     * La riga con la causale accorciata e' una riga scritta: conta come N, non come fallita, e il
     * suo warning e' aggregato. E' la semantica che il cliente ha chiesto.
     */
    @Test
    void laCausaleTroncataNonFaFallireLaRigaEVieneAggregata() {
        givenDatabase();
        FdrWriteSession session = session(10);

        session.accept(List.of(flusso("R0", "a".repeat(35)), flusso("R1", "corta"),
                flusso("R2", "b".repeat(31))));
        FdrWriteResult result = session.complete();

        assertThat(result.rowsInserted()).isEqualTo(3);
        assertThat(result.rowsRejected()).isZero();
        assertThat(result.truncation().affectedRows()).isEqualTo(2);
        assertThat(result.truncation().maxSourceLength()).isEqualTo(35);
        assertThat(result.truncation().persistedLength()).isEqualTo(30);
        assertThat(result.truncation().field()).isEqualTo("CAUSALE");
        assertThat(result.truncation().sampleRowOrdinals()).containsExactly(1L, 3L);
    }

    /** Il valore che arriva al database e' quello accorciato, e solo quello. */
    @Test
    void ilValoreBindatoELaCausaleAccorciata() throws SQLException {
        List<String> causali = new ArrayList<>();
        when(jdbcTemplate.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenAnswer(invocation -> {
                    BatchPreparedStatementSetter setter = invocation.getArgument(1);
                    for (int i = 0; i < setter.getBatchSize(); i++) {
                        causali.add((String) BoundValues.of(setter, i).get(6));
                    }
                    return new int[setter.getBatchSize()];
                });
        FdrWriteSession session = session(10);

        session.accept(List.of(flusso("R0", "a".repeat(35))));

        assertThat(causali).containsExactly("a".repeat(30));
    }

    @Test
    void senzaTroncamentiIlRiepilogoEVuoto() {
        givenDatabase();
        FdrWriteSession session = session(10);

        session.accept(List.of(flusso("R0", "corta")));

        assertThat(session.complete().truncation().isEmpty()).isTrue();
    }

    @Test
    void ilCampioneDeiTroncamentiELimitatoMaIlConteggioNo() {
        givenDatabase();
        FdrWriteSession session = session(40);

        session.accept(IntStream.range(0, 30)
                .mapToObj(i -> flusso("R" + i, "x".repeat(31)))
                .map(FdrBusinessRow.class::cast)
                .toList());
        FdrWriteResult result = session.complete();

        assertThat(result.truncation().affectedRows()).isEqualTo(30);
        assertThat(result.truncation().sampleRowOrdinals())
                .hasSize(FdrTruncationTally.MAX_SAMPLES);
    }

    // ─── Dataset e chiusura ──────────────────────────────────────────────────────

    /**
     * Una riga del dataset sbagliato non viene scritta nella tabella sbagliata: il controllo
     * precede il cast dentro lo statement, cosi' il guasto ha un nome invece di essere una
     * ClassCastException.
     */
    @Test
    void unaRigaDiUnAltroDatasetVieneRifiutataConUnNome() {
        FdrWriteSession session = session(10);

        assertThatThrownBy(() -> session.accept(List.of(FdrInsertStatementTest.iuv())))
                .isInstanceOf(FdrPersistenceException.class)
                .extracting(e -> ((FdrPersistenceException) e).kind())
                .isEqualTo(FdrPersistenceException.Kind.DATASET_MISMATCH);
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void laSessioneIuvScriveSullaTabellaIuv() {
        givenDatabase();
        FdrWriteSession session = new FdrWriteSession(
                new FdrWriteContext(FdrDataset.IUV_RENDICONTATI, DAY, "20260915/iuv-1.csv.gz"),
                jdbcTemplate, transactionTemplate, "sert_ingestor", 10);

        session.accept(List.of(FdrInsertStatementTest.iuv()));

        verify(jdbcTemplate).batchUpdate(
                org.mockito.ArgumentMatchers.contains("FDR_IUV_RENDICONTATI"),
                any(BatchPreparedStatementSetter.class));
    }

    /** Chiamare due volte la chiusura non raddoppia il file nei log ne' nei conteggi. */
    @Test
    void laChiusuraEIdempotente() {
        givenDatabase();
        FdrWriteSession session = session(10);
        session.accept(rows(2));

        FdrWriteResult first = session.complete();
        FdrWriteResult second = session.complete();

        assertThat(second).isSameAs(first);
    }

    // ─── Impalcatura ─────────────────────────────────────────────────────────────

    /**
     * Il finto database guarda le righe davvero bindate e rifiuta quelle marcate, con lo SQLSTATE
     * 22001 che PostgreSQL usa per "value too long".
     */
    // ─── Contatori a meta' lavoro ────────────────────────────────────────────────

    /**
     * Lo snapshot dei contatori e' leggibile mentre la sessione e' ancora aperta.
     *
     * <p>Serve al log per file, che deve raccontare un file in corso: senza, una giornata
     * interrotta mostrerebbe zeri e "non ha letto niente" sarebbe indistinguibile da "si e' fermato
     * quasi alla fine".
     */
    @Test
    void iContatoriSonoLeggibiliMentreLaSessioneEancoraAperta() {
        givenDatabase();
        FdrWriteSession session = session(1000);

        session.accept(rows(4));
        FdrWriteProgress progress = session.progress();

        assertThat(progress.rowsReceived()).isEqualTo(4);
        assertThat(progress.rowsInserted()).isEqualTo(4);
        assertThat(progress.rowsRejected()).isZero();
        assertThat(progress.rowsTruncated()).isZero();
    }

    /** Prima del primo batch non c'e' nulla da raccontare, e lo snapshot dice zero. */
    @Test
    void primaDelPrimoBatchLoSnapshotEvuoto() {
        FdrWriteProgress progress = session(1000).progress();

        assertThat(progress.rowsReceived()).isZero();
        assertThat(progress.rowsInserted()).isZero();
        assertThat(progress.rowsRejected()).isZero();
        assertThat(progress.rowsTruncated()).isZero();
    }

    /**
     * Lo snapshot cresce batch dopo batch e resta coerente: ricevute = inserite + rifiutate.
     *
     * <p>E' l'invariante che rende leggibile una riga di log ancora aperta. Vale a meta' lavoro
     * esattamente come a file chiuso, ed e' la ragione per cui il log puo' scrivere quei numeri
     * senza doverli etichettare come provvisori.
     */
    @Test
    void loSnapshotRestaCoerenteAOgniBatch() {
        givenDatabase();
        rejected.add("R2");
        FdrWriteSession session = session(2);

        session.accept(rows(2));
        FdrWriteProgress afterFirst = session.progress();
        session.accept(rows(4).subList(2, 4));
        FdrWriteProgress afterSecond = session.progress();

        assertThat(afterFirst.rowsReceived()).isEqualTo(2);
        assertThat(afterSecond.rowsReceived()).isEqualTo(4);
        assertThat(afterSecond.rowsReceived())
                .isEqualTo(afterSecond.rowsInserted() + afterSecond.rowsRejected());
        assertThat(afterSecond.rowsRejected()).isPositive();
    }

    /** Le causali accorciate compaiono nello snapshot, ma come righe riuscite. */
    @Test
    void loSnapshotDistingueLeCausaliAccorciateDagliScarti() {
        givenDatabase();
        FdrWriteSession session = session(1000);

        session.accept(List.of(flusso("R0", "x".repeat(40))));
        FdrWriteProgress progress = session.progress();

        assertThat(progress.rowsTruncated()).isEqualTo(1);
        assertThat(progress.rowsInserted()).isEqualTo(1);
        assertThat(progress.rowsRejected()).isZero();
    }

    /** Leggere lo snapshot non chiude nulla: la sessione resta usabile. */
    @Test
    void leggereLoSnapshotNonChiudeLaSessione() {
        givenDatabase();
        FdrWriteSession session = session(1000);

        session.accept(rows(2));
        session.progress();
        session.accept(rows(4).subList(2, 4));

        assertThat(session.complete().rowsInserted()).isEqualTo(4);
    }

    private void givenDatabase() {
        when(jdbcTemplate.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenAnswer(invocation -> {
                    BatchPreparedStatementSetter setter = invocation.getArgument(1);
                    List<String> batch = new ArrayList<>();
                    for (int i = 0; i < setter.getBatchSize(); i++) {
                        batch.add((String) BoundValues.of(setter, i).get(ID_FLUSSO_PARAMETER));
                    }
                    attempts.add(batch);
                    if (batch.stream().anyMatch(rejected::contains)) {
                        throw new DataIntegrityViolationException("rejected",
                                new SQLException(
                                        "value too long for type character varying(30)", "22001"));
                    }
                    return new int[batch.size()];
                });
    }

    /** Le righe effettivamente arrivate a destinazione: i tentativi riusciti. */
    private List<String> insertedRows() {
        return attempts.stream()
                .filter(batch -> batch.stream().noneMatch(rejected::contains))
                .flatMap(List::stream)
                .toList();
    }

    private FdrWriteSession session(int batchSize) {
        return new FdrWriteSession(
                new FdrWriteContext(FdrDataset.FLUSSI_RENDICONTAZIONE, DAY,
                        "20260915/FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz"),
                jdbcTemplate, transactionTemplate, "sert_ingestor", batchSize);
    }

    private static List<FdrBusinessRow> rows(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> flusso("R" + i, "causale"))
                .map(FdrBusinessRow.class::cast)
                .toList();
    }

    private static FdrFlussoRow flusso(String idFlusso, String causale) {
        return new FdrFlussoRow(DAY, LocalDateTime.parse("2026-09-15T08:38:01"), idFlusso,
                LocalDateTime.parse("2026-09-14T22:00:00"), null, causale, 1,
                new BigDecimal("34.51"), "80001070673", "PSP", "INT", "u-" + idFlusso, "[]");
    }
}
