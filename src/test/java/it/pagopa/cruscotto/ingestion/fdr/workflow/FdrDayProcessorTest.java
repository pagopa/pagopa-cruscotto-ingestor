package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrBatchHandler;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileFormatException;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReadResult;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReader;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrRowMapper;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrRowRejection;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrPersistenceException;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrRowWriter;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrTruncationSummary;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteResult;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteSession;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobContentReader;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobDiscoveryService;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDayDiscovery;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrStorageException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Il core di giornata: discovery, due dataset, esito.
 *
 * <p>Tutti i collaboratori sono finti perché quello che si verifica qui non è la lettura né la
 * scrittura — hanno i loro test — ma le <strong>decisioni</strong>: quando una giornata è
 * completata, quando è solo da guardare, quando vale la pena ritentare e quando no. Sono le regole
 * che il cliente leggerà come stato nella GUI.
 */
class FdrDayProcessorTest {

    private static final long BATCH = 7L;
    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final Instant NOW = Instant.parse("2026-11-18T10:00:00Z");

    private final FdrBlobDiscoveryService discovery = mock(FdrBlobDiscoveryService.class);
    private final FdrBlobContentReader contentReader = mock(FdrBlobContentReader.class);
    private final FdrFileReader fileReader = mock(FdrFileReader.class);
    private final FdrRowWriter rowWriter = mock(FdrRowWriter.class);
    private final FdrIngestionStatusRepository statusRepository =
            mock(FdrIngestionStatusRepository.class);
    private final FdrIngestionEventRepository eventRepository =
            mock(FdrIngestionEventRepository.class);
    private final FdrRowErrorStaging rowErrorStaging = mock(FdrRowErrorStaging.class);

    private FdrDayProcessor processor;

    @BeforeEach
    void setUp() {
        processor = newProcessor(5);
        when(statusRepository.find(anyLong(), any(), any())).thenReturn(Optional.empty());
        when(statusRepository.markProcessing(anyLong(), any(), any(), any()))
                .thenAnswer(invocation -> new FdrIngestionStatusRepository.FdrStatusRow(
                        invocation.getArgument(2) == FdrDataset.FLUSSI_RENDICONTAZIONE ? 1L : 2L,
                        FdrProcessingStatus.PROCESSING, 1));
    }

    private FdrDayProcessor newProcessor(int maxAttempts) {
        FdrProperties properties = new FdrProperties();
        properties.getRetry().setMaxAttempts(maxAttempts);
        return new FdrDayProcessor(discovery, contentReader, fileReader, rowWriter,
                statusRepository, eventRepository, rowErrorStaging, new FdrRetryPolicy(properties),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    // ─── Percorso felice ─────────────────────────────────────────────────────────

    @Test
    void conEntrambiIDatasetCompletiLaGiornataECompletata() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0), write(100, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED);
        assertThat(day.rowsSource()).isEqualTo(200);
        assertThat(day.rowsProcessed()).isEqualTo(200);
        assertThat(day.skipped()).isFalse();
        assertThat(day.datasets()).hasSize(2);
    }

    /**
     * La giornata elaborata e' quella passata, non quella di oggi. Qui l'orologio e' fermo al 18
     * novembre e la giornata e' il 15 settembre: se il core guardasse l'orologio, BOOTSTRAP e
     * REINGESTION di giornate storiche sarebbero impossibili.
     */
    @Test
    void laGiornataElaborataEQuellaPassataNonQuellaCorrente() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.businessDay()).isEqualTo(DAY);
        verify(discovery).discover(DAY);
    }

    // ─── Warning ─────────────────────────────────────────────────────────────────

    /**
     * 40.570 causali accorciate - il numero di una giornata reale - producono <strong>un</strong>
     * evento, non 40.570. La tabella di audit serve a leggere la giornata, non a copiarla.
     */
    @Test
    void quarantamilaTroncamentiProduconoUnSoloEventoAggregato() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(186857, 186857, 0),
                write(186857, 0, new FdrTruncationSummary("CAUSALE", 40570, 35, 30, List.of())));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        verify(eventRepository, times(2)).record(anyLong(), any(), any(), any(), any(),
                eq(FdrEventType.SOURCE_VALUE_TRUNCATED), any(), any(), any(), any(), any());
    }

    /** La riga con la causale accorciata resta contata in N: e' scritta, non fallita. */
    @Test
    void laCausaleTroncataNonRiduceN() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0),
                write(100, 0, new FdrTruncationSummary("CAUSALE", 21, 35, 30, List.of())));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.rowsProcessed()).isEqualTo(200);
        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).rowsFailed()).isZero();
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
    }

    /**
     * Un file vuoto accanto a uno pieno non fa fallire il dataset: gli export sono prodotti a
     * intervalli fissi e una finestra senza traffico produce esattamente questo.
     */
    @Test
    void unFileVuotoAccantoAUnoPienoLasciaIlDatasetInWarning() {
        FdrBlobFile pieno = file(FdrDataset.FLUSSI_RENDICONTAZIONE);
        FdrBlobFile vuoto = file(FdrDataset.FLUSSI_RENDICONTAZIONE, 2);
        FdrBlobFile iuv = file(FdrDataset.IUV_RENDICONTATI);
        givenDiscovery(List.of(pieno, vuoto), List.of(iuv));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(fileReader.read(any(), eq(pieno), any(), any())).thenReturn(read(100, 100, 0));
        when(fileReader.read(any(), eq(vuoto), any(), any())).thenReturn(read(0, 0, 0));
        when(fileReader.read(any(), eq(iuv), any(), any())).thenReturn(read(50, 50, 0));
        when(rowWriter.openSession(pieno))
                .thenAnswer(invocation -> session(write(100, 0, FdrTruncationSummary.none())));
        when(rowWriter.openSession(vuoto))
                .thenAnswer(invocation -> session(write(0, 0, FdrTruncationSummary.none())));
        when(rowWriter.openSession(iuv))
                .thenAnswer(invocation -> session(write(50, 0, FdrTruncationSummary.none())));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).status())
                .isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).rowsSource())
                .isEqualTo(100);
        verify(eventRepository).record(anyLong(), any(), any(),
                eq(FdrDataset.FLUSSI_RENDICONTAZIONE), eq(vuoto.blobName()),
                eq(FdrEventType.EMPTY_SOURCE_FILE), any(), any(), any(), any(), any());
    }

    @Test
    void leRigheScartateInLetturaLascianoIlDatasetInWarningEFinisconoInStaging() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenSessions();
        when(fileReader.read(any(), any(), any(), any())).thenReturn(read(100, 95, 5));
        when(rowWriter.openSession(any(FdrBlobFile.class)))
                .thenAnswer(invocation -> session(write(95, 0, FdrTruncationSummary.none())));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).rowsFailed()).isEqualTo(5);
        verify(rowErrorStaging, atLeastOnce()).stage(anyString(), any(), any(), any());
    }

    @Test
    void leRigheRifiutateDalDatabaseLascianoIlDatasetInWarning() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0), write(97, 3, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        assertThat(day.datasets().get(FdrDataset.IUV_RENDICONTATI).rowsProcessed()).isEqualTo(97);
        assertThat(day.datasets().get(FdrDataset.IUV_RENDICONTATI).rowsFailed()).isEqualTo(3);
    }

    /**
     * N viene dal writer, non dal lettore. Se venisse da {@code mappedRows}, una giornata in cui il
     * database rifiuta tutto risulterebbe completata.
     */
    @Test
    void nVieneDalleRigheScritteNonDaQuelleMappate() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0), write(0, 100, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.rowsProcessed()).isZero();
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).errorCode())
                .isEqualTo("ALL_ROWS_FAILED");
    }

    // ─── Dataset assente o vuoto ─────────────────────────────────────────────────

    @Test
    void unDatasetSenzaFileEMissingENonUnFallimentoTecnico() {
        givenDiscovery(List.of(file(FdrDataset.FLUSSI_RENDICONTAZIONE)), List.of());
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.datasets().get(FdrDataset.IUV_RENDICONTATI).status())
                .isEqualTo(FdrProcessingStatus.MISSING);
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
    }

    @Test
    void unDatasetConSoliFileVuotiFallisceSenzaRetry() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(0, 0, 0), write(0, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).errorCode())
                .isEqualTo("EMPTY_DATASET");
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
    }

    // ─── Guasti tecnici ──────────────────────────────────────────────────────────

    @Test
    void unGuastoDiAzureMetteLaGiornataInAttesaDiRitentare() {
        when(discovery.discover(DAY)).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.LISTING_FAILED, "listing failed", null));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.WAITING_RETRY);
        verify(statusRepository, times(2)).complete(anyLong(), any(), eq(NOW),
                eq(NOW.plusSeconds(3600)));
    }

    /** Il container inesistente e' configurazione: nessun numero di tentativi lo crea. */
    @Test
    void unContainerInesistenteNonVieneRitentato() {
        when(discovery.discover(DAY)).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.CONTAINER_NOT_FOUND, "no container", null));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
    }

    @Test
    void unGuastoDelDatabaseMetteLaGiornataInAttesaDiRitentare() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenSessions();
        when(fileReader.read(any(), any(), any(), any()))
                .thenThrow(new DataAccessResourceFailureException("db down"));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.WAITING_RETRY);
    }

    /** Partizione mancante: configurazione, non guasto. Ritentare non la crea. */
    @Test
    void unaPartizioneMancanteFallisceSenzaRitentare() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class))).thenThrow(new FdrPersistenceException(
                FdrPersistenceException.Kind.MISSING_PARTITION, "no partition"));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
    }

    /** Un header che non corrisponde e' un cambio di contratto: cinque riletture non lo cambiano. */
    @Test
    void unErroreStrutturaleDelFileFallisceSenzaRitentare() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenSessions();
        when(fileReader.read(any(), any(), any(), any())).thenThrow(new FdrFileFormatException(
                FdrFileFormatException.Kind.UNEXPECTED_HEADER, "header mismatch"));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
        verify(eventRepository, atLeastOnce()).record(anyLong(), any(), any(), any(), any(),
                eq(FdrEventType.FILE_STRUCTURAL_ERROR), any(), any(), any(), any(), any());
    }

    /** Un archivio corrotto invece e' il download interrotto: quello si ritenta. */
    @Test
    void unArchivioCorrottoVieneRitentato() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenSessions();
        when(fileReader.read(any(), any(), any(), any())).thenThrow(new FdrFileFormatException(
                FdrFileFormatException.Kind.CORRUPT_ARCHIVE, "truncated gzip"));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.WAITING_RETRY);
    }

    // ─── Tentativi esauriti ──────────────────────────────────────────────────────

    /**
     * All'ultimo tentativo l'esito diventa terminale e {@code NEXT_ATTEMPT_AT} resta vuoto: un
     * poller che vi trovasse una data riprenderebbe per sempre una giornata che nessuno vuole piu'
     * ritentare.
     */
    @Test
    void esauritiITentativiLEsitoDiventaTerminaleESenzaProssimoTentativo() {
        processor = newProcessor(5);
        when(statusRepository.markProcessing(anyLong(), any(), any(), any()))
                .thenReturn(new FdrIngestionStatusRepository.FdrStatusRow(1L,
                        FdrProcessingStatus.PROCESSING, 5));
        when(discovery.discover(DAY)).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.LISTING_FAILED, "listing failed", null));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).errorCode())
                .isEqualTo("RETRY_EXHAUSTED");
        verify(statusRepository, times(2)).complete(anyLong(), any(), eq(NOW), isNull());
        verify(eventRepository, times(2)).record(anyLong(), any(), any(), any(), any(),
                eq(FdrEventType.RETRY_EXHAUSTED), any(), any(), any(), any(), any());
    }

    /** Il massimo dei tentativi viene dalla configurazione, non dal codice. */
    @Test
    void ilMassimoDeiTentativiVieneDallaConfigurazione() {
        processor = newProcessor(1);
        when(statusRepository.markProcessing(anyLong(), any(), any(), any()))
                .thenReturn(new FdrIngestionStatusRepository.FdrStatusRow(1L,
                        FdrProcessingStatus.PROCESSING, 1));
        when(discovery.discover(DAY)).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.LISTING_FAILED, "listing failed", null));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
    }

    // ─── Idempotenza ─────────────────────────────────────────────────────────────

    /**
     * Una giornata gia' chiusa non si rielabora: la scrittura e' un INSERT fedele senza deduplica,
     * quindi rifarla raddoppierebbe le righe. Sovrascrivere una giornata e' la REINGESTION, e va
     * chiesta.
     */
    @Test
    void unaGiornataGiaCompletataNonVieneRielaborata() {
        when(statusRepository.find(anyLong(), any(), any())).thenReturn(
                Optional.of(new FdrIngestionStatusRepository.FdrStatusRow(1L,
                        FdrProcessingStatus.COMPLETED, 1)));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.skipped()).isTrue();
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED);
        verify(discovery, never()).discover(any());
        verify(statusRepository, never()).markProcessing(anyLong(), any(), any(), any());
    }

    /** Uno stato non terminale invece si riprende: e' esattamente il caso del retry. */
    @Test
    void unaGiornataInAttesaDiRitentareVieneRipresa() {
        when(statusRepository.find(anyLong(), any(), any())).thenReturn(
                Optional.of(new FdrIngestionStatusRepository.FdrStatusRow(1L,
                        FdrProcessingStatus.WAITING_RETRY, 2)));
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(BATCH, DAY);

        assertThat(day.skipped()).isFalse();
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED);
        verify(discovery).discover(DAY);
    }

    // ─── Stato persistito ────────────────────────────────────────────────────────

    @Test
    void loStatoPersistitoPortaXNEFallite() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0), write(98, 2, FdrTruncationSummary.none()));

        processor.process(BATCH, DAY);

        ArgumentCaptor<FdrDatasetResult> captor = ArgumentCaptor.forClass(FdrDatasetResult.class);
        verify(statusRepository, times(2)).complete(anyLong(), captor.capture(), any(), any());
        assertThat(captor.getAllValues()).allSatisfy(result -> {
            assertThat(result.rowsSource()).isEqualTo(100);
            assertThat(result.rowsProcessed()).isEqualTo(98);
            assertThat(result.rowsFailed()).isEqualTo(2);
        });
    }

    /** Una riga di stato per dataset, non una per file: il perimetro e' il dataset giornaliero. */
    @Test
    void unaSolaRigaDiStatoPerDataset() {
        givenDiscovery(List.of(file(FdrDataset.FLUSSI_RENDICONTAZIONE),
                        file(FdrDataset.FLUSSI_RENDICONTAZIONE, 2)),
                List.of(file(FdrDataset.IUV_RENDICONTATI)));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        processor.process(BATCH, DAY);

        verify(statusRepository, times(2)).markProcessing(anyLong(), any(), any(), any());
        verify(statusRepository, times(2)).complete(anyLong(), any(), any(), any());
    }

    // ─── Impalcatura ─────────────────────────────────────────────────────────────

    private void givenDiscovery(FdrBlobFile flussi, FdrBlobFile iuv) {
        givenDiscovery(List.of(flussi), List.of(iuv));
    }

    private void givenDiscovery(List<FdrBlobFile> flussi, List<FdrBlobFile> iuv) {
        Map<FdrDataset, List<FdrBlobFile>> files = new EnumMap<>(FdrDataset.class);
        files.put(FdrDataset.FLUSSI_RENDICONTAZIONE, flussi);
        files.put(FdrDataset.IUV_RENDICONTATI, iuv);
        when(discovery.discover(DAY))
                .thenReturn(new FdrDayDiscovery(DAY, "fdr", "20260915/", files, List.of()));
    }

    private void givenSessions() {
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class)))
                .thenAnswer(invocation -> session(write(0, 0, FdrTruncationSummary.none())));
    }

    private void givenFile(FdrFileReadResult read, FdrWriteResult write) {
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class)))
                .thenAnswer(invocation -> session(write));
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenReturn(read);
    }

    private static FdrWriteSession session(FdrWriteResult result) {
        FdrWriteSession session = mock(FdrWriteSession.class);
        when(session.complete()).thenReturn(result);
        return session;
    }

    private static FdrFileReadResult read(long dataRows, long mapped, long rejected) {
        List<FdrRowRejection> sample = rejected == 0 ? List.of()
                : List.of(new FdrRowRejection(2, "SOMMA_VERSATA",
                FdrRowRejection.Reason.INVALID_VALUE, "not a decimal"));
        return new FdrFileReadResult(FdrDataset.FLUSSI_RENDICONTAZIONE, "20260915/f.csv.gz", DAY,
                dataRows, mapped, rejected, 0, Map.of(), sample, 10);
    }

    private static FdrWriteResult write(long inserted, long rejected,
                                        FdrTruncationSummary truncation) {
        return new FdrWriteResult(FdrDataset.FLUSSI_RENDICONTAZIONE, DAY, "20260915/f.csv.gz",
                inserted + rejected, inserted, rejected, 1, 1, 0, truncation, List.of(), 10);
    }

    private static FdrBlobFile file(FdrDataset dataset) {
        return file(dataset, 1);
    }

    private static FdrBlobFile file(FdrDataset dataset, int progressive) {
        return new FdrBlobFile("20260915/" + dataset.filePrefix() + "_20260915-" + progressive
                + ".csv.gz", dataset, DAY, BigInteger.valueOf(progressive), -1L, null, null);
    }

    private void verifyNoRetryScheduled() {
        verify(eventRepository, never()).record(anyLong(), any(), any(), any(), any(),
                eq(FdrEventType.RETRY_SCHEDULED), any(), any(), any(), any(), any());
        verify(statusRepository, never()).complete(anyLong(), any(), any(),
                eq(NOW.plusSeconds(3600)));
    }
}
