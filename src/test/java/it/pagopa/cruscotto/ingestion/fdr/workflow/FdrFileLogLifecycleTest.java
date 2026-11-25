package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrBatchHandler;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrBusinessRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileFormatException;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReadResult;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReader;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrRowMapper;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrRowWriter;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrTruncationSummary;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteProgress;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteResult;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteSession;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobContentReader;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobDiscoveryService;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDayDiscovery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Il ciclo di vita della riga di log di un file, dentro il processore di giornata.
 *
 * <p>Qui vive una sola garanzia, e vale tutta la classe: <strong>l'ordine</strong>. La riga si apre
 * prima della lettura e si chiude dopo che la scrittura è tornata. Invertire il primo rende
 * invisibile l'unico caso per cui il log esiste — un file iniziato e mai finito perché il pod è
 * morto a metà; invertire il secondo permette di dichiarare {@code COMPLETED} su dati che non sono
 * mai stati scritti, che è una bugia che nessuno scoprirebbe.
 *
 * <p>Il cliente ha scelto di non avere staging business: è esattamente questa scelta a rendere il
 * log necessario, perché senza di esso una giornata parzialmente scritta non è distinguibile da una
 * completa.
 */
class FdrFileLogLifecycleTest {

    private static final long BATCH = 7L;
    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final Instant NOW = Instant.parse("2026-11-25T10:00:00Z");

    private final FdrBlobDiscoveryService discovery = mock(FdrBlobDiscoveryService.class);
    private final FdrBlobContentReader contentReader = mock(FdrBlobContentReader.class);
    private final FdrFileReader fileReader = mock(FdrFileReader.class);
    private final FdrRowWriter rowWriter = mock(FdrRowWriter.class);
    private final FdrIngestionStatusRepository statusRepository =
            mock(FdrIngestionStatusRepository.class);
    private final FdrIngestionEventRepository eventRepository =
            mock(FdrIngestionEventRepository.class);
    private final FdrRowErrorStaging rowErrorStaging = mock(FdrRowErrorStaging.class);
    private final FdrIngestionFileRepository fileRepository =
            mock(FdrIngestionFileRepository.class);

    private FdrDayProcessor processor;

    @BeforeEach
    void setUp() {
        FdrProperties properties = new FdrProperties();
        processor = new FdrDayProcessor(discovery, contentReader, fileReader, rowWriter,
                statusRepository, eventRepository, rowErrorStaging, fileRepository,
                new FdrRetryPolicy(properties), Clock.fixed(NOW, ZoneOffset.UTC));

        when(statusRepository.find(anyLong(), any(), any())).thenReturn(Optional.empty());
        when(statusRepository.markProcessing(anyLong(), any(), any(), any()))
                .thenAnswer(invocation -> new FdrIngestionStatusRepository.FdrStatusRow(
                        invocation.getArgument(2) == FdrDataset.FLUSSI_RENDICONTAZIONE ? 1L : 2L,
                        FdrProcessingStatus.PROCESSING, 1));
        when(fileRepository.begin(anyLong(), any(), any(), anyInt(), any())).thenReturn(50L);
    }

    // ─── Ordine ──────────────────────────────────────────────────────────────────

    /**
     * Apertura prima della lettura, chiusura dopo la scrittura.
     *
     * <p>È la forma che rende impossibile il caso peggiore. Spostare la chiusura prima di
     * {@code session.complete()} fa fallire questo test, ed è il motivo per cui esiste.
     */
    @Test
    void laRigaSiAprePrimaDellaLetturaESiChiudeDopoLaScrittura() {
        FdrWriteSession session = givenFile(read(100, 100, 0), write(100, 0));

        processor.process(BATCH, DAY);

        InOrder order = inOrder(fileRepository, contentReader, fileReader, session);
        order.verify(fileRepository).begin(eq(BATCH), any(), any(), anyInt(), any());
        order.verify(contentReader).open(any());
        order.verify(fileReader).read(any(), any(), any(FdrRowMapper.class),
                any(FdrBatchHandler.class));
        order.verify(session).complete();
        order.verify(fileRepository).complete(eq(50L), any(), any(), any());

        // Esattamente una chiusura per file — due dataset, due file, due chiusure. L'ordine da solo
        // non basterebbe: una chiusura anticipata seguita da una corretta rispetterebbe comunque la
        // sequenza, e la prima avrebbe gia' dichiarato completo un file non ancora scritto.
        verify(fileRepository, times(2)).complete(anyLong(), any(), any(), any());
        verify(fileRepository, times(2)).begin(anyLong(), any(), any(), anyInt(), any());
    }

    /** Il tentativo del log è quello della giornata: una fonte sola, non due contatori. */
    @Test
    void ilTentativoDelLogVieneDalloStatoDellaGiornata() {
        when(statusRepository.markProcessing(anyLong(), any(), any(), any()))
                .thenAnswer(invocation -> new FdrIngestionStatusRepository.FdrStatusRow(
                        1L, FdrProcessingStatus.PROCESSING, 3));
        givenFile(read(10, 10, 0), write(10, 0));

        processor.process(BATCH, DAY);

        verify(fileRepository, times(2)).begin(eq(BATCH), any(), any(), eq(3), any());
    }

    // ─── Contatori ───────────────────────────────────────────────────────────────

    /** File completo con qualche scarto: il totale è noto e i tre numeri tornano. */
    @Test
    void unFileCompletoConScartiRiportaTotaleLetteEScartate() {
        givenFile(read(100, 98, 2), write(98, 0));

        processor.process(BATCH, DAY);

        FdrFileCounters counters = capturedCompletion();
        assertThat(counters.rowsSource()).isEqualTo(100L);
        assertThat(counters.rowsRead()).isEqualTo(100L);
        assertThat(counters.rowsProcessed()).isEqualTo(98L);
        assertThat(counters.rowsFailed()).isEqualTo(2L);
        assertThat(counters.rowsSource())
                .as("a totale noto l'invariante X = N + falliti deve reggere")
                .isEqualTo(counters.rowsProcessed() + counters.rowsFailed());
    }

    @Test
    void unFileInteramenteRiuscitoNonHaScarti() {
        givenFile(read(100, 100, 0), write(100, 0));

        processor.process(BATCH, DAY);

        FdrFileCounters counters = capturedCompletion();
        assertThat(counters.rowsSource()).isEqualTo(100L);
        assertThat(counters.rowsProcessed()).isEqualTo(100L);
        assertThat(counters.rowsFailed()).isZero();
        assertThat(capturedStatus()).isEqualTo(FdrProcessingStatus.COMPLETED);
    }

    /**
     * Un file con il solo header è un esito legittimo, non un guasto.
     *
     * <p>Gli export sono prodotti a intervalli fissi e una finestra senza transiti ne produce uno.
     * Il totale è <strong>zero e noto</strong>, che è un'altra cosa dal totale ignoto di un file
     * interrotto.
     */
    @Test
    void unFileConIlSoloHeaderHaTotaleZeroENonEunGuasto() {
        givenFile(read(0, 0, 0), write(0, 0));

        processor.process(BATCH, DAY);

        FdrFileCounters counters = capturedCompletion();
        assertThat(counters.rowsSource()).isZero();
        assertThat(counters.rowsRead()).isZero();
        assertThat(counters.rowsProcessed()).isZero();
        assertThat(counters.rowsFailed()).isZero();
        assertThat(capturedStatus()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        verify(fileRepository, never()).fail(anyLong(), any(), any(), any(), any());
    }

    /**
     * Il caso che giustifica due contatori invece di uno.
     *
     * <p>Lettura interrotta a quarantamila righe su un file che ne aveva chissà quante: il totale
     * <strong>non si conosce</strong> e deve restare nullo. Scriverci il parziale farebbe sembrare
     * quel file un file da quarantamila righe completato quasi per intero — cioè trasformerebbe una
     * giornata rotta in una giornata apparentemente sana.
     */
    @Test
    void unaLetturaInterrottaNonSpacciaIlParzialePerIlTotale() {
        FdrWriteSession session = mock(FdrWriteSession.class);
        when(session.progress()).thenReturn(new FdrWriteProgress(40_000, 39_998, 2, 0));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class))).thenReturn(session);
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenThrow(new FdrFileFormatException(FdrFileFormatException.Kind.CORRUPT_ARCHIVE,
                        "gzip troncato"));
        givenDiscovery();

        processor.process(BATCH, DAY);

        ArgumentCaptor<FdrFileCounters> counters = ArgumentCaptor.forClass(FdrFileCounters.class);
        verify(fileRepository, org.mockito.Mockito.atLeastOnce())
                .fail(eq(50L), counters.capture(), anyString(), any(), any());
        FdrFileCounters partial = counters.getValue();

        assertThat(partial.rowsSource())
                .as("il totale di un file interrotto non lo conosce nessuno")
                .isNull();
        assertThat(partial.rowsRead()).isEqualTo(40_000L);
        assertThat(partial.rowsProcessed()).isEqualTo(39_998L);
        assertThat(partial.rowsFailed()).isEqualTo(2L);
        verify(fileRepository, never()).complete(anyLong(), any(), any(), any());
    }

    /**
     * Le causali accorciate sono righe <strong>riuscite</strong>.
     *
     * <p>Contarle fra i falliti sarebbe il modo più diretto per far sembrare rotta una giornata
     * normale: sui dati reali il troncamento tocca una riga su cinque, quindi il 21% di ogni
     * giornata risulterebbe scartato.
     */
    @Test
    void leCausaliAccorciateContanoFraLeRiusciteNonFraGliScarti() {
        givenFile(read(100, 100, 0), write(100, 0,
                new FdrTruncationSummary("CAUSALE", 10, 35, 30, List.of())));

        processor.process(BATCH, DAY);

        FdrFileCounters counters = capturedCompletion();
        assertThat(counters.rowsTruncated()).isEqualTo(10L);
        assertThat(counters.rowsProcessed())
                .as("le dieci righe accorciate sono comunque arrivate in banca dati")
                .isEqualTo(100L);
        assertThat(counters.rowsFailed()).isZero();
        assertThat(capturedStatus()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
    }

    /** Un file letto per intero da cui non è arrivata nessuna riga non è un warning: è fallito. */
    @Test
    void unFileDaCuiNonEarrivatoNullaRisultaFallito() {
        givenFile(read(100, 100, 0), write(0, 100));

        processor.process(BATCH, DAY);

        assertThat(capturedStatus()).isEqualTo(FdrProcessingStatus.FAILED);
    }

    // ─── Costo ───────────────────────────────────────────────────────────────────

    /**
     * Il costo è per batch, mai per riga.
     *
     * <p>Con un aggiornamento per riga un file da tre milioni di righe vorrebbe dire tre milioni di
     * UPDATE accanto a tre milioni di INSERT: il log costerebbe quanto il dato. Con uno per batch il
     * rapporto diventa 1 a mille, e lo scopo — sapere dove ci si è fermati — resta intatto.
     */
    @Test
    void ilProgressoCostaUnUpdatePerBatchNonPerRiga() {
        FdrWriteSession session = mock(FdrWriteSession.class);
        when(session.complete()).thenReturn(write(3_000, 0));
        when(session.progress()).thenReturn(new FdrWriteProgress(1_000, 1_000, 0, 0));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class))).thenReturn(session);
        // Tre batch da mille righe ciascuno. I batch devono contenere PIU' di una riga, altrimenti
        // "un aggiornamento per batch" e "uno per riga" darebbero lo stesso numero e il test non
        // distinguerebbe le due cose.
        List<FdrBusinessRow> rows = java.util.stream.IntStream.range(0, 1_000)
                .mapToObj(index -> mock(FdrBusinessRow.class))
                .toList();
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenAnswer(invocation -> {
                    FdrBatchHandler<FdrBusinessRow> handler = invocation.getArgument(3);
                    for (int batch = 0; batch < 3; batch++) {
                        handler.accept(rows);
                    }
                    return read(3_000, 3_000, 0);
                });
        givenDiscovery();

        processor.process(BATCH, DAY);

        // Tre batch per ciascuno dei due dataset: sei aggiornamenti, non seimila.
        verify(fileRepository, times(6)).progress(eq(50L), any(), any());
    }

    // ─── Guasto dell'audit ───────────────────────────────────────────────────────

    /**
     * Se la chiusura del log fallisce, il dato business resta e la giornata <strong>non</strong>
     * fallisce.
     *
     * <p>È la decisione meno ovvia del disegno, e la ragione è concreta: a quel punto le righe sono
     * già committate, quindi far fallire il dataset significherebbe un retry che le
     * <em>riscriverebbe</em> — la scrittura è un INSERT fedele, senza deduplica. Un problema di
     * tracciatura diventerebbe una giornata duplicata, cioè un danno al dato vero.
     *
     * <p>Quello che resta è il verso sicuro: la riga rimane aperta e sotto-dichiara, in modo
     * visibile, e non viene mai dichiarata completa.
     */
    @Test
    void unGuastoNelChiudereIlLogNonFaFallireLaGiornataNeRollbackaIlBusiness() {
        givenFile(read(100, 100, 0), write(100, 0));
        doThrow(new org.springframework.dao.DataAccessResourceFailureException("audit down"))
                .when(fileRepository).complete(anyLong(), any(), any(), any());

        FdrDayResult result = processor.process(BATCH, DAY);

        assertThat(result.status())
                .as("un guasto dell'audit non deve rendere ritentabile una giornata gia' scritta")
                .isEqualTo(FdrProcessingStatus.COMPLETED);
        verify(fileRepository, never()).fail(anyLong(), any(), any(), any(), any());
    }

    /** Un guasto della scrittura business, invece, deve propagarsi: la giornata non è a posto. */
    @Test
    void unGuastoDellaScritturaBusinessSiPropagaDopoAverChiusoIlLogComeFallito() {
        FdrWriteSession session = mock(FdrWriteSession.class);
        when(session.progress()).thenReturn(new FdrWriteProgress(10, 5, 5, 0));
        when(session.complete()).thenThrow(new IllegalStateException("connection reset"));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class))).thenReturn(session);
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenReturn(read(10, 10, 0));
        givenDiscovery();

        FdrDayResult result = processor.process(BATCH, DAY);

        verify(fileRepository, org.mockito.Mockito.atLeastOnce())
                .fail(eq(50L), any(), eq("IllegalStateException"), anyString(), any());
        verify(fileRepository, never()).complete(anyLong(), any(), any(), any());
        assertThat(result.status()).isNotEqualTo(FdrProcessingStatus.COMPLETED);
    }

    /**
     * Un guasto nel registrare il fallimento non deve <strong>sostituire</strong> quello vero.
     *
     * <p>È il guasto originale a dire se la giornata è ritentabile: un archivio corrotto sì, un
     * header incompatibile no. Se al suo posto arrivasse un errore dell'audit, la classificazione
     * cambierebbe nella direzione peggiore — un errore inatteso non è ritentabile, quindi una
     * giornata recuperabile verrebbe chiusa come terminale e servirebbe un intervento manuale.
     *
     * <p>Questo caso non è teorico: è stato trovato dalla suite esistente, dove la prima stesura
     * del lifecycle trasformava un {@code WAITING_RETRY} in un {@code FAILED}.
     */
    @Test
    void unGuastoDellAuditNonSostituisceIlGuastoVeroNeCambiaLaRitentabilita() {
        FdrWriteSession session = mock(FdrWriteSession.class);
        when(session.progress()).thenThrow(new IllegalStateException("audit snapshot down"));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class))).thenReturn(session);
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenThrow(new FdrFileFormatException(FdrFileFormatException.Kind.CORRUPT_ARCHIVE,
                        "gzip troncato"));
        givenDiscovery();

        FdrDayResult result = processor.process(BATCH, DAY);

        assertThat(result.status())
                .as("un archivio corrotto resta ritentabile anche se l'audit non riesce a scriverlo")
                .isEqualTo(FdrProcessingStatus.WAITING_RETRY);
    }

    /**
     * Un guasto nell'aggiornare il progresso <strong>non deve fermare il file</strong>.
     *
     * <p>E' il caso piu' insidioso dei tre, per via di dove gira quel codice: dentro il gestore dei
     * batch del lettore. Un'eccezione li' interrompe la lettura, e lo fa dopo che i batch precedenti
     * sono gia' stati committati — il file risulterebbe fallito, la giornata ritentabile, e il
     * tentativo successivo riscriverebbe quelle stesse righe, perche' la scrittura e' un INSERT
     * fedele senza deduplica. Un UPDATE di audit andato storto diventerebbe una giornata duplicata.
     */
    @Test
    void unGuastoNellAggiornareIlProgressoNonFermaIlFile() {
        FdrWriteSession session = mock(FdrWriteSession.class);
        when(session.complete()).thenReturn(write(2_000, 0));
        when(session.progress()).thenReturn(new FdrWriteProgress(1_000, 1_000, 0, 0));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class))).thenReturn(session);
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenAnswer(invocation -> {
                    FdrBatchHandler<FdrBusinessRow> handler = invocation.getArgument(3);
                    handler.accept(List.of(mock(FdrBusinessRow.class)));
                    handler.accept(List.of(mock(FdrBusinessRow.class)));
                    return read(2_000, 2_000, 0);
                });
        doThrow(new org.springframework.dao.DataAccessResourceFailureException("audit down"))
                .when(fileRepository).progress(anyLong(), any(), any());
        givenDiscovery();

        FdrDayResult result = processor.process(BATCH, DAY);

        assertThat(result.status())
                .as("il file deve arrivare in fondo: fermarlo qui duplicherebbe la giornata al"
                        + " tentativo successivo")
                .isEqualTo(FdrProcessingStatus.COMPLETED);
        // Due batch per ciascuno dei due dataset: la lettura e' arrivata in fondo nonostante
        // l'audit rotto a ogni singolo batch.
        verify(session, times(4)).accept(any());
        verify(fileRepository, never()).fail(anyLong(), any(), any(), any(), any());
    }

    /**
     * Anche un guasto nello scrivere gli eventi del file non fa fallire la giornata.
     *
     * <p>Stessa posizione e stessa conseguenza della chiusura della riga: a quel punto il dato
     * business e' committato, e rendere il dataset ritentabile significherebbe riscriverlo. La
     * protezione copre il blocco intero perche' un evento e un UPDATE falliscono per le stesse
     * ragioni, non perche' uno sia piu' importante dell'altro.
     */
    @Test
    void unGuastoNelloScrivereGliEventiDelFileNonFaFallireLaGiornata() {
        givenFile(read(100, 98, 2), write(98, 0));
        doThrow(new org.springframework.dao.DataAccessResourceFailureException("events down"))
                .when(eventRepository).record(anyLong(), any(), any(), any(), any(), anyString(),
                        anyString(), anyString(), anyString(), any(), any());

        FdrDayResult result = processor.process(BATCH, DAY);

        assertThat(result.status()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        verify(fileRepository, never()).fail(anyLong(), any(), any(), any(), any());
    }

    // ─── Il log non è un checkpoint ──────────────────────────────────────────────

    /**
     * Una riga {@code COMPLETED} non autorizza nessuno a saltare quel file.
     *
     * <p>Questo test non protegge un difetto attuale: protegge da un'ottimizzazione futura. Saltare
     * i file già completati sembra gratis, ma richiede una garanzia di coerenza fra il commit del
     * dato business e il commit di questa riga che non esiste — e dove non esiste, saltare un file
     * significa perderlo. Il processore non deve mai <em>leggere</em> il log.
     */
    @Test
    void ilLogNonVieneMaiLettoPerDecidereCosaElaborare() {
        givenFile(read(100, 100, 0), write(100, 0));

        processor.process(BATCH, DAY);

        // Il contratto pubblico del repository e' fatto di sole scritture: aprire, aggiornare,
        // chiudere. Finche' non espone una lettura, nessuno puo' usarlo per decidere cosa saltare —
        // e se un giorno una lettura servisse davvero, questo test obbliga ad aggiungerla di
        // proposito invece che per distrazione.
        assertThat(java.util.Arrays.stream(FdrIngestionFileRepository.class.getDeclaredMethods())
                .filter(method -> java.lang.reflect.Modifier.isPublic(method.getModifiers()))
                .map(java.lang.reflect.Method::getName)
                .toList())
                .as("il repository del log non deve esporre letture")
                .containsExactlyInAnyOrder("begin", "progress", "complete", "fail");
        verify(fileReader, times(2)).read(any(), any(), any(FdrRowMapper.class),
                any(FdrBatchHandler.class));
    }

    /**
     * Lo stato del dataset viene dai totali dell'elaborazione, non da una somma sul log.
     *
     * <p>Se dipendesse dal log, una scrittura di audit persa abbasserebbe in silenzio i contatori
     * operativi del dataset — e quelli decidono se la giornata è completa.
     */
    @Test
    void loStatoDelDatasetNonDipendeDalLogPerFile() {
        givenFile(read(100, 98, 2), write(98, 0));
        doThrow(new IllegalStateException("audit down"))
                .when(fileRepository).complete(anyLong(), any(), any(), any());

        processor.process(BATCH, DAY);

        ArgumentCaptor<FdrDatasetResult> result = ArgumentCaptor.forClass(FdrDatasetResult.class);
        verify(statusRepository, org.mockito.Mockito.atLeastOnce())
                .complete(anyLong(), result.capture(), any(), any());
        assertThat(result.getValue().rowsSource())
                .as("i contatori del dataset vengono dall'elaborazione, non dal log")
                .isEqualTo(100L);
        assertThat(result.getValue().rowsProcessed()).isEqualTo(98L);
    }

    // ─── Utilità ─────────────────────────────────────────────────────────────────

    private FdrWriteSession givenFile(FdrFileReadResult read, FdrWriteResult write) {
        FdrWriteSession session = mock(FdrWriteSession.class);
        when(session.complete()).thenReturn(write);
        when(session.progress()).thenReturn(new FdrWriteProgress(write.rowsReceived(),
                write.rowsInserted(), write.rowsRejected(), write.truncation().affectedRows()));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class))).thenReturn(session);
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenReturn(read);
        givenDiscovery();
        return session;
    }

    private void givenDiscovery() {
        Map<FdrDataset, List<FdrBlobFile>> files = new EnumMap<>(FdrDataset.class);
        files.put(FdrDataset.FLUSSI_RENDICONTAZIONE,
                List.of(file(FdrDataset.FLUSSI_RENDICONTAZIONE)));
        files.put(FdrDataset.IUV_RENDICONTATI, List.of(file(FdrDataset.IUV_RENDICONTATI)));
        when(discovery.discover(DAY))
                .thenReturn(new FdrDayDiscovery(DAY, "fdr", "20260915/", files, List.of()));
    }

    private FdrFileCounters capturedCompletion() {
        ArgumentCaptor<FdrFileCounters> counters = ArgumentCaptor.forClass(FdrFileCounters.class);
        verify(fileRepository, org.mockito.Mockito.atLeastOnce())
                .complete(eq(50L), any(), counters.capture(), any());
        return counters.getValue();
    }

    private FdrProcessingStatus capturedStatus() {
        ArgumentCaptor<FdrProcessingStatus> status =
                ArgumentCaptor.forClass(FdrProcessingStatus.class);
        verify(fileRepository, org.mockito.Mockito.atLeastOnce())
                .complete(eq(50L), status.capture(), any(), any());
        return status.getValue();
    }

    private static FdrFileReadResult read(long dataRows, long mapped, long rejected) {
        return new FdrFileReadResult(FdrDataset.FLUSSI_RENDICONTAZIONE, "20260915/f.csv.gz", DAY,
                dataRows, mapped, rejected, 0, Map.of(), List.of(), 10);
    }

    private static FdrWriteResult write(long inserted, long rejected) {
        return write(inserted, rejected, FdrTruncationSummary.none());
    }

    private static FdrWriteResult write(long inserted, long rejected,
                                        FdrTruncationSummary truncation) {
        return new FdrWriteResult(FdrDataset.FLUSSI_RENDICONTAZIONE, DAY, "20260915/f.csv.gz",
                inserted + rejected, inserted, rejected, 1, 1, 0, truncation, List.of(), 10);
    }

    private static FdrBlobFile file(FdrDataset dataset) {
        return new FdrBlobFile("20260915/" + dataset.filePrefix() + "_20260915-1.csv.gz",
                dataset, DAY, BigInteger.ONE, -1L, null, null);
    }
}
