package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * La lettura di un file, dal GZIP alle righe mappate.
 *
 * <p>I fixture sono costruiti qui e compressi in memoria: nessun file di esempio sul filesystem,
 * nessuna Azure. Quello che si verifica non e' solo il caso nominale ma soprattutto i modi in cui
 * un file puo' essere rotto — perche' la regola del cliente e' che un FDR anomalo vada ingerito e
 * segnalato, non rifiutato, e la riga di confine fra "riga da scartare" e "file da fermare" e'
 * l'unica decisione davvero delicata di questo componente.
 */
class FdrFileReaderTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    private static final String FLUSSI_HEADER = "\"ID_FLUSSO\",\"DATA_ORA_FLUSSO\","
            + "\"INSERTED_TIMESTAMP\",\"DATA_REGOLAMENTO\",\"CAUSALE\",\"NUM_PAGAMENTI\","
            + "\"SOMMA_VERSATA\",\"ID_DOMINIO\",\"PSP\",\"INT_PSP\",\"UNIQUE_ID\",\"ALL_DATES\"";

    private static final String FLUSSI_ROW = "\"2026-09-14IFSPIT21-CU20260000000WX4\","
            + "\"2026-09-14T22:00:00.000Z\",\"2026-09-15T08:38:01.7616300Z\","
            + "\"2026-09-14T00:00:00.000Z\",\"3604220120371326480320003200IT\",\"1\",\"34.51\","
            + "\"80001070673\",\"IFSPIT21\",\"14847241008\","
            + "\"50ea4624-ccbc-462a-96f3-5b2726f81ae3\",\"[\"\"2026-09-14T00:00:00.0000000Z\"\"]\"";

    private final FdrFileReader reader = readerWithBatchSize(1000);

    // ─── Caso nominale ───────────────────────────────────────────────────────────

    @Test
    void leggeUnFileEConsegnaLeRigheMappate() {
        List<FdrFlussoRow> rows = new ArrayList<>();

        FdrFileReadResult result = readFlussi(gzip(join(FLUSSI_HEADER, FLUSSI_ROW, FLUSSI_ROW)),
                batch -> rows.addAll(batch));

        assertThat(result.dataRows()).isEqualTo(2);
        assertThat(result.mappedRows()).isEqualTo(2);
        assertThat(result.rejectedRows()).isZero();
        assertThat(result.isComplete()).isTrue();
        assertThat(result.isEmptySource()).isFalse();
        assertThat(result.businessDay()).isEqualTo(DAY);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).causale()).isEqualTo("3604220120371326480320003200IT");
        assertThat(rows.get(0).dateInserted()).isEqualTo(DAY);
    }

    /**
     * BOM in testa e terminatori CRLF: le due varianti convivono nello stesso giorno, prodotte
     * dallo stesso export lanciato con parametri diversi. Un file non deve fallire per un
     * carattere invisibile ne' per un ritorno a capo.
     */
    @Test
    void leggeUnFileConBomECrlf() {
        String content = "﻿" + FLUSSI_HEADER + "\r\n" + FLUSSI_ROW + "\r\n";

        FdrFileReadResult result = readFlussi(gzip(content), batch -> { });

        assertThat(result.dataRows()).isEqualTo(1);
        assertThat(result.mappedRows()).isEqualTo(1);
    }

    /**
     * Un file valido senza righe non e' un fallimento: gli export sono prodotti a intervalli fissi
     * e una finestra senza traffico produce esattamente questo. Diventa un problema solo se
     * <em>tutti</em> i file del dataset sono cosi', ed e' una valutazione della giornata.
     */
    @Test
    void unFileConIlSoloHeaderNonEUnErrore() {
        AtomicBoolean handlerCalled = new AtomicBoolean();

        FdrFileReadResult result = readFlussi(gzip(FLUSSI_HEADER),
                batch -> handlerCalled.set(true));

        assertThat(result.isEmptySource()).isTrue();
        assertThat(result.dataRows()).isZero();
        assertThat(result.isComplete()).isTrue();
        assertThat(handlerCalled).isFalse();
    }

    /**
     * Le righe vuote sono contate a parte e non entrano nella X: un file che diventasse per meta'
     * di righe vuote sarebbe un file diverso, e quel contatore e' l'unica cosa che lo mostrerebbe.
     */
    @Test
    void leRigheVuoteSonoContateAParteENonEntranoNellaX() {
        FdrFileReadResult result = readFlussi(
                gzip(join(FLUSSI_HEADER, FLUSSI_ROW, "", "", FLUSSI_ROW)), batch -> { });

        assertThat(result.dataRows()).isEqualTo(2);
        assertThat(result.blankLines()).isEqualTo(2);
    }

    // ─── Batching ────────────────────────────────────────────────────────────────

    @Test
    void leRigheEsconoABatchDellaDimensioneConfigurata() {
        List<Integer> batchSizes = new ArrayList<>();
        String content = join(FLUSSI_HEADER, FLUSSI_ROW, FLUSSI_ROW, FLUSSI_ROW, FLUSSI_ROW,
                FLUSSI_ROW);

        FdrFileReadResult result = readFlussi(readerWithBatchSize(2), gzip(content),
                batch -> batchSizes.add(batch.size()));

        assertThat(batchSizes).containsExactly(2, 2, 1);
        assertThat(result.mappedRows()).isEqualTo(5);
    }

    /**
     * Ogni batch e' una lista nuova. Riusarne una sola risparmierebbe un'allocazione ogni mille
     * righe e introdurrebbe il difetto di aliasing: un handler che conserva il batch — e la
     * persistenza potrebbe farlo — se lo ritroverebbe svuotato sotto.
     */
    @Test
    void ogniBatchEUnaListaDistinta() {
        List<List<FdrFlussoRow>> batches = new ArrayList<>();
        String content = join(FLUSSI_HEADER, FLUSSI_ROW, FLUSSI_ROW, FLUSSI_ROW);

        readFlussi(readerWithBatchSize(1), gzip(content), batches::add);

        assertThat(batches).hasSize(3);
        assertThat(batches.get(0)).isNotSameAs(batches.get(1));
        assertThat(batches).allSatisfy(batch -> assertThat(batch).hasSize(1));
    }

    /**
     * Un fallimento dell'handler non e' una riga sbagliata: e' la destinazione che rifiuta il dato.
     * Andare avanti vorrebbe dire leggere un file intero per non scriverne nulla.
     */
    @Test
    void unErroreDellHandlerFermaIlFile() {
        String content = join(FLUSSI_HEADER, FLUSSI_ROW, FLUSSI_ROW);

        assertThatThrownBy(() -> readFlussi(readerWithBatchSize(1), gzip(content), batch -> {
            throw new IllegalStateException("insert failed");
        })).isInstanceOf(IllegalStateException.class).hasMessage("insert failed");
    }

    // ─── Righe da scartare, file che prosegue ────────────────────────────────────

    @Test
    void unaRigaMalformataVieneScartataELaLetturaProsegue() {
        String broken = "\"a\",\"b";
        FdrFileReadResult result = readFlussi(
                gzip(join(FLUSSI_HEADER, FLUSSI_ROW, broken, FLUSSI_ROW)), batch -> { });

        assertThat(result.dataRows()).isEqualTo(3);
        assertThat(result.mappedRows()).isEqualTo(2);
        assertThat(result.rejectedRows()).isEqualTo(1);
        assertThat(result.rejectionsByReason())
                .containsEntry(FdrRowRejection.Reason.MALFORMED_LINE, 1L);
    }

    @Test
    void unaRigaTroncataVieneClassificataComeCampiMancanti() {
        FdrFileReadResult result = readFlussi(
                gzip(join(FLUSSI_HEADER, "\"solo\",\"due\"")), batch -> { });

        assertThat(result.rejectionsByReason())
                .containsEntry(FdrRowRejection.Reason.FIELD_COUNT_MISMATCH, 1L);
    }

    @Test
    void unValoreNonConvertibileVieneClassificatoComeValoreInvalido() {
        String badAmount = FLUSSI_ROW.replace("\"34.51\"", "\"non-un-numero\"");

        FdrFileReadResult result = readFlussi(gzip(join(FLUSSI_HEADER, badAmount)), batch -> { });

        assertThat(result.rejectionsByReason())
                .containsEntry(FdrRowRejection.Reason.INVALID_VALUE, 1L);
        assertThat(result.rejectionSample()).singleElement()
                .satisfies(rejection -> {
                    assertThat(rejection.column()).isEqualTo(FdrColumns.SOMMA_VERSATA);
                    assertThat(rejection.lineNumber()).isEqualTo(2);
                });
    }

    /** X = N + scartate, su ogni file. E' l'invariante su cui si decide lo stato della giornata. */
    @Test
    void leRigheLetteSonoSempreLaSommaDiMappateEScartate() {
        String content = join(FLUSSI_HEADER, FLUSSI_ROW, "\"rotta", FLUSSI_ROW, "\"corta\"");

        FdrFileReadResult result = readFlussi(gzip(content), batch -> { });

        assertThat(result.dataRows()).isEqualTo(result.mappedRows() + result.rejectedRows());
    }

    /**
     * I conteggi sono completi, il campione e' limitato. Senza il tetto, un dataset esportato con
     * un formato di timestamp diverso — tutte le righe scartate — costruirebbe un oggetto per
     * riga, e il lettore che esiste per limitare la memoria sarebbe quello che la esaurisce.
     */
    @Test
    void ilCampioneDelleRigheScartateELimitatoMaIConteggiNo() {
        List<String> lines = new ArrayList<>();
        lines.add(FLUSSI_HEADER);
        IntStream.range(0, 25).forEach(i -> lines.add("\"rotta"));

        FdrFileReadResult result = readFlussi(gzip(String.join("\n", lines)), batch -> { });

        assertThat(result.rejectedRows()).isEqualTo(25);
        assertThat(result.rejectionsByReason())
                .containsEntry(FdrRowRejection.Reason.MALFORMED_LINE, 25L);
        assertThat(result.rejectionSample()).hasSize(FdrFileReader.MAX_SAMPLED_REJECTIONS);
    }

    // ─── File che si fermano ─────────────────────────────────────────────────────

    @Test
    void unFileCompletamenteVuotoNonHaHeader() {
        assertThatThrownBy(() -> readFlussi(gzip(""), batch -> { }))
                .isInstanceOf(FdrFileFormatException.class)
                .extracting(e -> ((FdrFileFormatException) e).kind())
                .isEqualTo(FdrFileFormatException.Kind.MISSING_HEADER);
    }

    @Test
    void unContenutoNonGzipVieneRiconosciutoComeArchivioCorrotto() {
        byte[] plain = join(FLUSSI_HEADER, FLUSSI_ROW).getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> readFlussi(plain, batch -> { }))
                .isInstanceOf(FdrFileFormatException.class)
                .extracting(e -> ((FdrFileFormatException) e).kind())
                .isEqualTo(FdrFileFormatException.Kind.CORRUPT_ARCHIVE);
    }

    /** Un download interrotto a meta' produce esattamente questo, ed e' il caso ritentabile. */
    @Test
    void unGzipTroncatoVieneRiconosciutoComeArchivioCorrotto() {
        byte[] full = gzip(join(FLUSSI_HEADER, FLUSSI_ROW, FLUSSI_ROW));
        byte[] truncated = Arrays.copyOf(full, full.length - 8);

        assertThatThrownBy(() -> readFlussi(truncated, batch -> { }))
                .isInstanceOf(FdrFileFormatException.class)
                .extracting(e -> ((FdrFileFormatException) e).kind())
                .isEqualTo(FdrFileFormatException.Kind.CORRUPT_ARCHIVE);
    }

    /**
     * Byte non validi in UTF-8 vengono segnalati, non sostituiti. Il decoder di default li
     * rimpiazzerebbe con U+FFFD e andrebbe avanti: righe che si leggono, si mappano e si
     * inseriscono, con gli identificativi alterati in silenzio.
     */
    @Test
    void unaCodificaNonValidaFermaIlFileInveceDiSostituireICaratteri() {
        byte[] content = concat(
                (FLUSSI_HEADER + "\n").getBytes(StandardCharsets.UTF_8),
                new byte[] {(byte) 0xC3, (byte) 0x28},
                "\n".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> readFlussi(gzip(content), batch -> { }))
                .isInstanceOf(FdrFileFormatException.class)
                .extracting(e -> ((FdrFileFormatException) e).kind())
                .isEqualTo(FdrFileFormatException.Kind.INVALID_ENCODING);
    }

    @Test
    void unHeaderDiAltroDatasetFermaIlFile() {
        String iuvHeader = "\"IUV\",\"IUR\",\"IMPORTO\"";

        assertThatThrownBy(() -> readFlussi(gzip(iuvHeader), batch -> { }))
                .isInstanceOf(FdrFileFormatException.class)
                .extracting(e -> ((FdrFileFormatException) e).kind())
                .isEqualTo(FdrFileFormatException.Kind.UNEXPECTED_HEADER);
    }

    @Test
    void unaLetturaInterrottaDelloStreamNonDiventaUnArchivioCorrotto() {
        InputStream failing = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("connection reset");
            }
        };

        assertThatThrownBy(() -> reader.read(failing, file(FdrDataset.FLUSSI_RENDICONTAZIONE),
                FdrFlussoRowMapper.INSTANCE, batch -> { }))
                .isInstanceOf(FdrFileFormatException.class)
                .extracting(e -> ((FdrFileFormatException) e).kind())
                .isEqualTo(FdrFileFormatException.Kind.READ_FAILED);
    }

    // ─── Lo stream viene sempre chiuso ───────────────────────────────────────────

    /**
     * Lo stream del blob e' una connessione HTTP: non chiuderlo le lascia aperte una per file, e
     * dopo qualche centinaio di file la giornata si ferma per esaurimento del pool. Vale anche —
     * soprattutto — quando il file e' illeggibile.
     */
    @Test
    void loStreamVieneChiusoSiaInSuccessoSiaInErrore() {
        ClosingStream ok = new ClosingStream(gzip(join(FLUSSI_HEADER, FLUSSI_ROW)));
        reader.read(ok, file(FdrDataset.FLUSSI_RENDICONTAZIONE), FdrFlussoRowMapper.INSTANCE,
                batch -> { });
        assertThat(ok.closed).isTrue();

        ClosingStream broken = new ClosingStream("non gzip".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> reader.read(broken, file(FdrDataset.FLUSSI_RENDICONTAZIONE),
                FdrFlussoRowMapper.INSTANCE, batch -> { }))
                .isInstanceOf(FdrFileFormatException.class);
        assertThat(broken.closed).isTrue();
    }

    // ─── IUV, con la forma a quoting misto ───────────────────────────────────────

    @Test
    void leggeUnFileIuvConQuotingMisto() {
        String header = "\"IUV\",\"IUR\",\"IMPORTO\",\"COD_ESITO\","
                + "\"DATA_ESITO_SINGOLO_PAGAMENTO\",\"IDSP\",\"ID_FLUSSO\",\"DATA_ORA_FLUSSO\","
                + "\"ID_DOMINIO\",\"PSP\",\"INT_PSP\",\"UNIQUE_ID\",\"INSERTED_TIMESTAMP\"";
        String row = "\"00000000002043975\",\"9043f52d9111458baac55b7aac85f6ac\",\"131\",0,"
                + "\"2026-09-13T00:00:00Z\",\"1\",\"2026-09-14ABI08811-0024220051715769\","
                + "\"2026-09-15T01:07:35Z\",\"00242200517\",\"ABI08811\",\"04774801007\","
                + "\"3cfb679a-3bfb-4c2d-9f50-5908de56d1bf\",\"2026-09-15T00:17:24.341577Z\"";
        List<FdrIuvRow> rows = new ArrayList<>();

        FdrFileReadResult result = reader.read(stream(gzip(join(header, row))),
                file(FdrDataset.IUV_RENDICONTATI), FdrIuvRowMapper.INSTANCE, rows::addAll);

        assertThat(result.mappedRows()).isEqualTo(1);
        assertThat(result.dataset()).isEqualTo(FdrDataset.IUV_RENDICONTATI);
        assertThat(rows).singleElement().satisfies(r -> {
            assertThat(r.iuv()).isEqualTo("00000000002043975");
            assertThat(r.codEsito()).isZero();
        });
    }

    // ─── Fixture ─────────────────────────────────────────────────────────────────

    private static FdrFileReader readerWithBatchSize(int batchSize) {
        FdrProperties properties = new FdrProperties();
        properties.getProcessing().setBatchSize(batchSize);
        return new FdrFileReader(properties);
    }

    private FdrFileReadResult readFlussi(byte[] content, FdrBatchHandler<FdrFlussoRow> handler) {
        return readFlussi(reader, content, handler);
    }

    private static FdrFileReadResult readFlussi(FdrFileReader reader, byte[] content,
                                                FdrBatchHandler<FdrFlussoRow> handler) {
        return reader.read(stream(content), file(FdrDataset.FLUSSI_RENDICONTAZIONE),
                FdrFlussoRowMapper.INSTANCE, handler);
    }

    private static FdrBlobFile file(FdrDataset dataset) {
        return new FdrBlobFile("20260915/" + dataset.filePrefix() + "_20260915-1.csv.gz",
                dataset, DAY, BigInteger.ONE, -1L, null, null);
    }

    private static InputStream stream(byte[] content) {
        return new ByteArrayInputStream(content);
    }

    private static String join(String... lines) {
        return Arrays.stream(lines).collect(Collectors.joining("\n"));
    }

    private static byte[] gzip(String content) {
        return gzip(content.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] gzip(byte[] content) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /** Uno stream che ricorda di essere stato chiuso. */
    private static final class ClosingStream extends ByteArrayInputStream {

        private boolean closed;

        private ClosingStream(byte[] content) {
            super(content);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
