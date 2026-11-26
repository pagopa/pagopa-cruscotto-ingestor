package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReadResult;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReader;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFlussoRowMapper;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrIuvRowMapper;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Il giunto fra i due step: file GZIP, lettore, scrittore, parametri SQL.
 *
 * <p>Fixture sintetiche e piccole, non i sample originali: quelli sono utili per la validazione
 * analitica ma legare la suite a file da dieci megabyte la renderebbe lenta e dipendente da
 * materiale che non e' versionato.
 *
 * <p>Quello che si verifica qui non si vede da nessuno dei due lati separatamente: che la causale
 * resti intera in lettura e arrivi accorciata solo al binding, che la giornata venga dalla cartella
 * e non dal contenuto, e che l'istante della sorgente attraversi la pipeline immutato.
 */
class FdrReaderToWriterTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    private static final String FLUSSI_HEADER = "\"ID_FLUSSO\",\"DATA_ORA_FLUSSO\","
            + "\"INSERTED_TIMESTAMP\",\"DATA_REGOLAMENTO\",\"CAUSALE\",\"NUM_PAGAMENTI\","
            + "\"SOMMA_VERSATA\",\"ID_DOMINIO\",\"PSP\",\"INT_PSP\",\"UNIQUE_ID\",\"ALL_DATES\"";

    /** Causale da 35 caratteri, la forma reale del 21,71% delle righe di una giornata. */
    private static final String CAUSALE_35 = "36042201203713264803200032001234567";

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final PlatformTransactionManager transactionManager =
            mock(PlatformTransactionManager.class);
    private final List<Map<Integer, Object>> bound = new ArrayList<>();

    FdrReaderToWriterTest() {
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        when(jdbcTemplate.batchUpdate(anyString(), any(BatchPreparedStatementSetter.class)))
                .thenAnswer(invocation -> {
                    BatchPreparedStatementSetter setter = invocation.getArgument(1);
                    for (int i = 0; i < setter.getBatchSize(); i++) {
                        bound.add(BoundValues.of(setter, i));
                    }
                    return new int[setter.getBatchSize()];
                });
    }

    @Test
    void unFileDiFlussiAttraversaLaPipelineFinoAiParametriSql() {
        String row = "\"2026-09-14IFSPIT21-CU20260000000WX4\",\"2026-09-14T22:00:00.000Z\","
                + "\"2026-09-15T08:38:01.7616300Z\",\"2026-09-14T00:00:00.000Z\","
                + "\"" + CAUSALE_35 + "\",\"1\",\"34.51\",\"80001070673\",\"IFSPIT21\","
                + "\"14847241008\",\"50ea4624-ccbc-462a-96f3-5b2726f81ae3\","
                + "\"[\"\"2026-09-14T00:00:00.0000000Z\"\"]\"";

        FdrWriteSession session = session(FdrDataset.FLUSSI_RENDICONTAZIONE);
        FdrFileReadResult read = reader().read(gzip(FLUSSI_HEADER + "\n" + row),
                file(FdrDataset.FLUSSI_RENDICONTAZIONE), FdrFlussoRowMapper.INSTANCE,
                session::accept);
        FdrWriteResult written = session.complete();

        assertThat(read.dataRows()).isEqualTo(1);
        assertThat(read.mappedRows()).isEqualTo(1);
        assertThat(written.rowsInserted()).isEqualTo(1);
        assertThat(written.rowsRejected()).isZero();

        Map<Integer, Object> parameters = bound.get(0);
        // La giornata e' quella della cartella, non la data che apre ID_FLUSSO - che e' il 14.
        assertThat(parameters.get(1)).isEqualTo(Date.valueOf(DAY));
        assertThat(parameters.get(3)).isEqualTo("2026-09-14IFSPIT21-CU20260000000WX4");
        // L'istante della sorgente arriva immutato, frazione di secondo compresa.
        assertThat(parameters.get(2))
                .isEqualTo(LocalDateTime.of(2026, 9, 15, 8, 38, 1, 761630000));
        assertThat(parameters.get(8)).isEqualTo(new BigDecimal("34.51"));
        assertThat(parameters.get(13)).isEqualTo("[\"2026-09-14T00:00:00.0000000Z\"]");
    }

    /**
     * La prova che il troncamento sta dove e' stato deciso: il lettore conserva i 35 caratteri, il
     * writer ne scrive 30, e la riga conta come scritta con un warning aggregato - non come riga
     * fallita.
     */
    @Test
    void laCausaleRestaInteraInLetturaEVieneAccorciataSoloInScrittura() {
        String row = "\"F1\",\"2026-09-14T22:00:00.000Z\",\"2026-09-15T08:38:01.000Z\","
                + "\"2026-09-14T00:00:00.000Z\",\"" + CAUSALE_35 + "\",\"1\",\"34.51\","
                + "\"8000\",\"PSP\",\"INT\",\"u-1\",\"[]\"";
        List<String> causaliLette = new ArrayList<>();

        FdrWriteSession session = session(FdrDataset.FLUSSI_RENDICONTAZIONE);
        reader().read(gzip(FLUSSI_HEADER + "\n" + row), file(FdrDataset.FLUSSI_RENDICONTAZIONE),
                FdrFlussoRowMapper.INSTANCE, batch -> {
                    batch.forEach(r -> causaliLette.add(r.causale()));
                    session.accept(batch);
                });
        FdrWriteResult written = session.complete();

        assertThat(causaliLette).containsExactly(CAUSALE_35);
        assertThat(bound.get(0).get(6)).isEqualTo(CAUSALE_35.substring(0, 30));
        assertThat(written.rowsInserted()).isEqualTo(1);
        assertThat(written.rowsRejected()).isZero();
        assertThat(written.truncation().affectedRows()).isEqualTo(1);
        assertThat(written.truncation().maxSourceLength()).isEqualTo(35);
    }

    /** Lo IUV con gli zeri iniziali sopravvive a lettura, mapping e binding. */
    @Test
    void unFileIuvConservaLoIuvZeroPaddedFinoAiParametri() {
        String header = "\"IUV\",\"IUR\",\"IMPORTO\",\"COD_ESITO\","
                + "\"DATA_ESITO_SINGOLO_PAGAMENTO\",\"IDSP\",\"ID_FLUSSO\",\"DATA_ORA_FLUSSO\","
                + "\"ID_DOMINIO\",\"PSP\",\"INT_PSP\",\"UNIQUE_ID\",\"INSERTED_TIMESTAMP\"";
        String row = "\"00000000002043975\",\"9043f52d9111458baac55b7aac85f6ac\",\"131\",0,"
                + "\"2026-09-13T00:00:00Z\",\"1\",\"2026-09-14ABI08811-0024220051715769\","
                + "\"2026-09-15T01:07:35Z\",\"00242200517\",\"ABI08811\",\"04774801007\","
                + "\"3cfb679a-3bfb-4c2d-9f50-5908de56d1bf\",\"2026-09-15T00:17:24.341577Z\"";

        FdrWriteSession session = session(FdrDataset.IUV_RENDICONTATI);
        reader().read(gzip(header + "\n" + row), file(FdrDataset.IUV_RENDICONTATI),
                FdrIuvRowMapper.INSTANCE, session::accept);
        FdrWriteResult written = session.complete();

        assertThat(written.rowsInserted()).isEqualTo(1);
        assertThat(bound.get(0).get(2)).isEqualTo("00000000002043975");
        assertThat(bound.get(0).get(5)).isEqualTo(0);
        assertThat(bound.get(0).get(14))
                .isEqualTo(LocalDateTime.of(2026, 9, 15, 0, 17, 24, 341577000));
    }

    /**
     * I duplicati esatti dei file reali arrivano in banca dati come righe distinte: nessuna
     * deduplica fra lettore e scrittore, che e' il requisito di fedelta' al sorgente.
     */
    @Test
    void leRigheDuplicateArrivanoTutteEDue() {
        String row = "\"F1\",\"2026-09-14T22:00:00.000Z\",\"2026-09-15T08:38:01.000Z\","
                + "\"2026-09-14T00:00:00.000Z\",\"causale\",\"1\",\"34.51\",\"8000\",\"PSP\","
                + "\"INT\",\"u-1\",\"[]\"";

        FdrWriteSession session = session(FdrDataset.FLUSSI_RENDICONTAZIONE);
        FdrFileReadResult read = reader().read(gzip(FLUSSI_HEADER + "\n" + row + "\n" + row),
                file(FdrDataset.FLUSSI_RENDICONTAZIONE), FdrFlussoRowMapper.INSTANCE,
                session::accept);

        assertThat(read.dataRows()).isEqualTo(2);
        assertThat(session.complete().rowsInserted()).isEqualTo(2);
        assertThat(bound).hasSize(2);
        assertThat(bound.get(0).get(12)).isEqualTo(bound.get(1).get(12));
    }

    /**
     * Una riga che il lettore scarta non arriva al writer: X resta 2, N diventa 1, e il writer
     * conferma di averne scritta una. E' la relazione che l'orchestrazione dovra' comporre.
     */
    @Test
    void unaRigaScartataInLetturaNonEntraNeiConteggiDelWriter() {
        String good = "\"F1\",\"2026-09-14T22:00:00.000Z\",\"2026-09-15T08:38:01.000Z\","
                + "\"2026-09-14T00:00:00.000Z\",\"causale\",\"1\",\"34.51\",\"8000\",\"PSP\","
                + "\"INT\",\"u-1\",\"[]\"";
        String badAmount = good.replace("\"34.51\"", "\"non-un-numero\"");

        FdrWriteSession session = session(FdrDataset.FLUSSI_RENDICONTAZIONE);
        FdrFileReadResult read = reader().read(gzip(FLUSSI_HEADER + "\n" + good + "\n" + badAmount),
                file(FdrDataset.FLUSSI_RENDICONTAZIONE), FdrFlussoRowMapper.INSTANCE,
                session::accept);
        FdrWriteResult written = session.complete();

        assertThat(read.dataRows()).isEqualTo(2);
        assertThat(read.mappedRows()).isEqualTo(1);
        assertThat(written.rowsReceived()).isEqualTo(1);
        assertThat(written.rowsInserted()).isEqualTo(1);
    }

    // ─── Impalcatura ─────────────────────────────────────────────────────────────

    private FdrFileReader reader() {
        FdrProperties properties = new FdrProperties();
        properties.getProcessing().setBatchSize(1000);
        return new FdrFileReader(properties);
    }

    private FdrWriteSession session(FdrDataset dataset) {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        return new FdrWriteSession(FdrWriteContext.of(file(dataset)), jdbcTemplate, template,
                "sert_ingestor", 1000, FdrDayFence.NONE);
    }

    private static FdrBlobFile file(FdrDataset dataset) {
        return new FdrBlobFile("20260915/" + dataset.filePrefix() + "_20260915-1.csv.gz",
                dataset, DAY, BigInteger.ONE, -1L, null, null);
    }

    private static ByteArrayInputStream gzip(String content) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ByteArrayInputStream(out.toByteArray());
    }
}
