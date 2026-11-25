package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrBatchHandler;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileFormatException;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReadResult;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReader;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrPartitionGuard;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrPersistenceException;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobContentReader;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobDiscoveryService;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDayDiscovery;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrStorageException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * L'unico punto di FDR che cancella dati, e le tre cose che glielo impediscono.
 *
 * <p>Ogni test qui corrisponde a un modo in cui una giornata verrebbe svuotata senza poter essere
 * riscritta. Non sono ipotesi astratte: il preflight esiste perche' una sorgente puo' mancare o
 * essere vuota, la guardia sulla partizione perche' fuori dall'orizzonte Liquibase le righe
 * finirebbero nella DEFAULT, e la guardia sulla DEFAULT perche' un {@code TRUNCATE} della
 * partizione esplicita non la tocca.
 */
class FdrReingestionPreparerTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    private final FdrBlobDiscoveryService discovery = mock(FdrBlobDiscoveryService.class);
    private final FdrBlobContentReader contentReader = mock(FdrBlobContentReader.class);
    private final FdrFileReader fileReader = mock(FdrFileReader.class);
    private final FdrPartitionGuard partitionGuard = mock(FdrPartitionGuard.class);
    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final PlatformTransactionManager transactionManager =
            mock(PlatformTransactionManager.class);

    private final FdrReingestionPreparer preparer = newPreparer();

    private FdrReingestionPreparer newPreparer() {
        DbSchemaConfig schema = new DbSchemaConfig();
        schema.setSchema("sert_ingestor");
        when(transactionManager.getTransaction(any())).thenReturn(mock(TransactionStatus.class));
        return new FdrReingestionPreparer(discovery, contentReader, fileReader, partitionGuard,
                jdbcTemplate, transactionManager, schema);
    }

    // ─── Preflight ───────────────────────────────────────────────────────────────

    @Test
    void conEntrambiIDatasetPieniLaGiornataEricostruib() {
        discoveryWith(files(FdrDataset.FLUSSI_RENDICONTAZIONE, 1),
                files(FdrDataset.IUV_RENDICONTATI, 1));
        readsReturning(true);

        assertThat(preparer.preflight(DAY).rebuildable()).isTrue();
    }

    @Test
    void unDatasetAssenteRendeLaGiornataNonRicostruibile() {
        for (FdrDataset missing : FdrDataset.values()) {
            FdrDataset present = other(missing);
            discoveryWith(files(present, 1), List.of());
            readsReturning(true);

            FdrReingestionPreparer.FdrPreflightResult result = newPreparer().preflight(DAY);

            assertThat(result.rebuildable()).as("manca %s", missing).isFalse();
            assertThat(result.reasonCode()).isEqualTo("DATASET_MISSING");
        }
    }

    /**
     * Un file vuoto fra file pieni non squalifica nulla.
     *
     * <p>Gli export sono prodotti a intervalli fissi e una finestra senza transiti ne produce uno
     * con il solo header: trattarlo come un guasto renderebbe non reingeribili giornate
     * perfettamente normali.
     */
    @Test
    void unFileVuotoFraFilePieniNonSqualificaIlDataset() {
        List<FdrBlobFile> flussi = files(FdrDataset.FLUSSI_RENDICONTAZIONE, 2);
        discoveryWith(flussi, files(FdrDataset.IUV_RENDICONTATI, 1));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        // Il primo file non porta righe, gli altri si'. Stubbing in forma doAnswer: la forma
        // when(mock.metodo(...)) *chiama* il mock, e su un metodo gia' stubbato con un Answer
        // eseguirebbe quell'Answer con argomenti nulli.
        doAnswer(invocation -> feedOneRow(invocation.getArgument(3)))
                .when(fileReader).read(any(), any(), any(), any());
        doReturn(emptyRead(flussi.get(0)))
                .when(fileReader).read(any(), eq(flussi.get(0)), any(), any());

        assertThat(preparer.preflight(DAY).rebuildable()).isTrue();
    }

    /**
     * Se invece sono vuoti tutti, la sorgente e' stata letta correttamente ed e' vuota.
     *
     * <p>E' la differenza fra "non lo so" e "lo so, ed e' no": rileggere lo stesso blob non
     * cambierebbe il contenuto, quindi non ha senso ritentare, e men che meno svuotare la giornata
     * per riscriverci zero righe.
     */
    @Test
    void unDatasetInteramenteVuotoNonSuperaIlPreflight() {
        discoveryWith(files(FdrDataset.FLUSSI_RENDICONTAZIONE, 2),
                files(FdrDataset.IUV_RENDICONTATI, 1));
        readsReturning(false);

        FdrReingestionPreparer.FdrPreflightResult result = preparer.preflight(DAY);

        assertThat(result.rebuildable()).isFalse();
        assertThat(result.reasonCode()).isEqualTo("EMPTY_DATASET");
    }

    /** Un file illeggibile non prova nulla: se nessun altro porta dati, il dataset non passa. */
    @Test
    void unFileStrutturalmenteRottoNonValeComeProvaDiRicostruibilita() {
        discoveryWith(files(FdrDataset.FLUSSI_RENDICONTAZIONE, 1),
                files(FdrDataset.IUV_RENDICONTATI, 1));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        doThrow(new FdrFileFormatException(FdrFileFormatException.Kind.CORRUPT_ARCHIVE, "not a gzip"))
                .when(fileReader).read(any(), any(), any(), any());

        assertThat(preparer.preflight(DAY).rebuildable()).isFalse();
    }

    /**
     * Un guasto di trasporto sale, non diventa un "no".
     *
     * <p>Azure irraggiungibile non dice nulla sui file. Convertirlo in "non ricostruibile"
     * renderebbe terminale una giornata che bastava ritentare.
     */
    @Test
    void unGuastoDiTrasportoNonVieneScambiatoPerUnDatasetAssente() {
        when(discovery.discover(DAY)).thenThrow(new FdrStorageException(FdrStorageException.Kind.LISTING_FAILED, "azure unreachable", null));

        assertThatThrownBy(() -> preparer.preflight(DAY))
                .isInstanceOf(FdrStorageException.class);
    }

    /**
     * Il preflight legge il meno possibile: un batch per file, e si ferma.
     *
     * <p>Leggere per intero due dataset da centinaia di megabyte per dirsi "si, ci sono dati"
     * raddoppierebbe il lavoro della giornata, su un'operazione che e' solo una verifica.
     */
    @Test
    void ilPreflightSiFermaAlPrimoBatchDiOgniFile() {
        discoveryWith(files(FdrDataset.FLUSSI_RENDICONTAZIONE, 3),
                files(FdrDataset.IUV_RENDICONTATI, 3));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        doAnswer(invocation -> feedOneRow(invocation.getArgument(3)))
                .when(fileReader).read(any(), any(), any(), any());

        preparer.preflight(DAY);

        // Un file per dataset: trovata la prova, gli altri due non vengono nemmeno aperti.
        verify(contentReader, org.mockito.Mockito.times(2)).open(any());
    }

    // ─── Guardie e truncate ──────────────────────────────────────────────────────

    /**
     * Un solo {@code TRUNCATE} per entrambe le partizioni, dentro una sola transazione.
     *
     * <p>Due statement con un commit in mezzo lascerebbero, se il secondo fallisse, una giornata
     * con meta' dei dati cancellati e l'altra meta' vecchia: lo stato peggiore possibile, perche'
     * sembra popolata.
     */
    @Test
    void leDuePartizioniVengonoSvuotateInsiemeInUnaSolaTransazione() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(0L);

        preparer.guardAndTruncate(DAY);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).execute(sql.capture());
        assertThat(sql.getValue())
                .startsWith("TRUNCATE TABLE ")
                .contains("sert_ingestor.\"FDR_FLUSSI_RENDICONTAZIONE_20260915\"")
                .contains("sert_ingestor.\"FDR_IUV_RENDICONTATI_20260915\"");

        InOrder order = inOrder(transactionManager, jdbcTemplate);
        order.verify(transactionManager).getTransaction(any());
        order.verify(jdbcTemplate).execute(anyString());
        order.verify(transactionManager).commit(any());
    }

    /**
     * I nomi sono citati perche' le partizioni giornaliere sono maiuscole.
     *
     * <p>Senza virgolette PostgreSQL le abbasserebbe a minuscolo e non le troverebbe. E' lo stesso
     * inciampo che ha tenuto rotto per mesi il job di retention dello staging, e si manifesta non
     * come un errore di sintassi ma come un {@code TRUNCATE} che non svuota nulla.
     */
    @Test
    void iNomiDellePartizioniSonoCitati() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(0L);

        preparer.guardAndTruncate(DAY);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).execute(sql.capture());
        assertThat(sql.getValue().chars().filter(c -> c == '"').count())
                .as("due nomi citati, quindi quattro virgolette")
                .isEqualTo(4);
        assertThat(sql.getValue()).doesNotContain("_DEFAULT").doesNotContain("_default");
    }

    @Test
    void senzaPartizioneEsplicitaNonSiTroncaNulla() {
        doThrow(new FdrPersistenceException(
                        FdrPersistenceException.Kind.MISSING_PARTITION, "no partition"))
                .when(partitionGuard).requireExplicitPartition(any(), eq(DAY));

        assertThatThrownBy(() -> preparer.guardAndTruncate(DAY))
                .isInstanceOf(FdrPersistenceException.class);
        verify(jdbcTemplate, never()).execute(anyString());
    }

    /**
     * La DEFAULT con righe della giornata ferma tutto.
     *
     * <p>Il {@code TRUNCATE} della partizione esplicita non le toccherebbe, quindi la reingestione
     * le sommerebbe ai dati nuovi: una giornata duplicata invece che sostituita. Non vengono
     * spostate in automatico — muovere dati di nascosto dentro un'operazione gia' distruttiva e' il
     * modo di trasformare un problema visibile in uno che non lo e'.
     */
    @Test
    void laDefaultConRigheDellaGiornataFermaIlTruncate() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(7L);

        assertThatThrownBy(() -> preparer.guardAndTruncate(DAY))
                .isInstanceOf(FdrPersistenceException.class)
                .hasMessageContaining("DEFAULT");
        verify(jdbcTemplate, never()).execute(anyString());
    }

    /** Entrambe le DEFAULT vengono interrogate: una sola lascerebbe scoperto l'altro dataset. */
    @Test
    void entrambeLeDefaultVengonoVerificate() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(0L);

        preparer.guardAndTruncate(DAY);

        verify(partitionGuard).requireExplicitPartition(FdrDataset.FLUSSI_RENDICONTAZIONE, DAY);
        verify(partitionGuard).requireExplicitPartition(FdrDataset.IUV_RENDICONTATI, DAY);
        verify(jdbcTemplate, org.mockito.Mockito.times(2))
                .queryForObject(anyString(), eq(Long.class), any());
    }

    /** Solo la giornata chiesta: il nome della partizione la contiene, e nessun'altra compare. */
    @Test
    void nessunAltraGiornataVieneToccata() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(0L);

        preparer.guardAndTruncate(DAY);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).execute(sql.capture());
        assertThat(sql.getValue()).doesNotContain("20260914").doesNotContain("20260916");
    }

    // ─── Utilita' ────────────────────────────────────────────────────────────────

    private void discoveryWith(List<FdrBlobFile> flussi, List<FdrBlobFile> iuv) {
        Map<FdrDataset, List<FdrBlobFile>> byDataset = new EnumMap<>(FdrDataset.class);
        byDataset.put(FdrDataset.FLUSSI_RENDICONTAZIONE, flussi);
        byDataset.put(FdrDataset.IUV_RENDICONTATI, iuv);
        when(discovery.discover(DAY))
                .thenReturn(new FdrDayDiscovery(DAY, "fdr", "20260915/", byDataset, List.of()));
    }

    /** Ogni lettura porta righe, oppure nessuna ne porta. */
    private void readsReturning(boolean withRows) {
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        if (withRows) {
            doAnswer(invocation -> feedOneRow(invocation.getArgument(3)))
                    .when(fileReader).read(any(), any(), any(), any());
        } else {
            doAnswer(invocation -> emptyRead(invocation.getArgument(1)))
                    .when(fileReader).read(any(), any(), any(), any());
        }
    }

    /**
     * Consegna un batch non vuoto all'handler, come farebbe il lettore vero.
     *
     * <p>E' il punto in cui il preflight si interrompe: l'handler lancia, il lettore propaga e il
     * file non viene letto oltre.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static FdrFileReadResult feedOneRow(Object handler) {
        ((FdrBatchHandler) handler).accept(List.of(mock(FdrBusinessRow.class)));
        throw new AssertionError("il preflight doveva interrompersi al primo batch");
    }

    private static FdrFileReadResult emptyRead(FdrBlobFile file) {
        return new FdrFileReadResult(file.dataset(), file.blobName(), file.businessDay(),
                0, 0, 0, 0, Map.of(), List.of(), 0);
    }

    private static List<FdrBlobFile> files(FdrDataset dataset, int count) {
        return java.util.stream.IntStream.rangeClosed(1, count)
                .mapToObj(progressive -> new FdrBlobFile(
                        "20260915/" + dataset.name() + "_20260915-" + progressive + ".csv.gz",
                        dataset, DAY, BigInteger.valueOf(progressive), 1024, "etag", null))
                .toList();
    }

    private static FdrDataset other(FdrDataset dataset) {
        return dataset == FdrDataset.FLUSSI_RENDICONTAZIONE
                ? FdrDataset.IUV_RENDICONTATI
                : FdrDataset.FLUSSI_RENDICONTAZIONE;
    }
}
