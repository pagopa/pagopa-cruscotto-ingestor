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
import java.time.Duration;
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
import static org.mockito.ArgumentMatchers.anyInt;
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
    private final FdrDayOwnership ownership = mock(FdrDayOwnership.class);
    private final FdrIngestionFileRepository fileRepository =
            mock(FdrIngestionFileRepository.class);

    private FdrDayProcessor processor;

    @BeforeEach
    void setUp() {
        processor = newProcessor(5);
        when(statusRepository.find(anyLong(), any(), any())).thenReturn(Optional.empty());
        // La chiusura e' permissiva di default: qui si verificano le decisioni di giornata, non il
        // possesso, che ha i suoi test in FdrDayOwnershipTest e FdrTransactionalFencingTest. I
        // test che vogliono la perdita del lease la stubbano da se'.
        when(ownership.guardFor(any())).thenAnswer(invocation -> new FdrLeaseGuard(
                ownership, invocation.getArgument(0), Duration.ofSeconds(30), NOW));
        when(ownership.renew(any(), any())).thenReturn(true);
    }

    private FdrDayProcessor newProcessor(int maxAttempts) {
        FdrProperties properties = new FdrProperties();
        properties.getRetry().setMaxAttempts(maxAttempts);
        return new FdrDayProcessor(discovery, contentReader, fileReader, rowWriter,
                statusRepository, eventRepository, ownership, fileRepository,
                new FdrRetryPolicy(properties), Clock.fixed(NOW, ZoneOffset.UTC));
    }

    /**
     * Il possesso già preso, con i due dataset mai visti prima.
     *
     * <p>Costruito a mano invece di farlo produrre dal claim vero: questi test riguardano ciò che
     * succede <em>dopo</em> la presa in carico, e passare dal claim li legherebbe a un secondo
     * comportamento — un suo cambiamento li farebbe fallire tutti insieme, nascondendo quale regola
     * si è davvero rotta.
     */
    private static FdrDayLease lease() {
        return lease(FdrProcessingStatus.PENDING, FdrProcessingStatus.PENDING);
    }

    /** Lo stesso possesso, a un tentativo scelto: serve ai test sul tetto dei ritentativi. */
    private static FdrDayLease leaseAtAttempt(int attemptNo) {
        return new FdrDayLease(BATCH, DAY, "token-1", attemptNo,
                Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, 1L, FdrDataset.IUV_RENDICONTATI, 2L),
                Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, FdrProcessingStatus.WAITING_RETRY,
                        FdrDataset.IUV_RENDICONTATI, FdrProcessingStatus.WAITING_RETRY),
                NOW.plusSeconds(300));
    }

    private static FdrDayLease lease(FdrProcessingStatus flussi, FdrProcessingStatus iuv) {
        return new FdrDayLease(BATCH, DAY, "token-1", 1,
                Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, 1L, FdrDataset.IUV_RENDICONTATI, 2L),
                Map.of(FdrDataset.FLUSSI_RENDICONTAZIONE, flussi, FdrDataset.IUV_RENDICONTATI, iuv),
                NOW.plusSeconds(300));
    }

    // ─── Battito ─────────────────────────────────────────────────────────────────

    /**
     * Il battito usa l'orologio, non la scadenza del lease.
     *
     * <p><strong>Difetto reale che questo test impedisce di reintrodurre.</strong> La prima stesura
     * passava {@code lease.leaseExpiresAt()} dove serviva l'istante corrente, e faceva due danni
     * insieme, entrambi silenziosi: il rinnovo scriveva "scadenza + durata" invece di "adesso +
     * durata", raddoppiando di fatto il lease, e l'istante dell'ultimo battito finiva nel futuro —
     * sopprimendo ogni battito successivo per tutta la durata del lease.
     *
     * <p>La correttezza reggeva comunque, perché la chiusura dentro ogni transazione di scrittura
     * non è limitata dall'intervallo; a morire era il presidio sulle fasi lente, cioè proprio
     * quello per cui il battito esiste. È il tipo di difetto che non fa fallire nulla e si scopre
     * quando una giornata viene sottratta a un worker vivo.
     *
     * <p>Con l'orologio fermo il tempo non passa, quindi il rinnovo opportunistico non deve
     * scattare affatto; e soprattutto non deve mai scattare con un istante <em>futuro</em>.
     */
    @Test
    void ilBattitoNonUsaMaiUnIstanteFuturo() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        processor.process(lease(), FdrDayScope.RESUME);

        verify(ownership, never()).renew(any(),
                org.mockito.ArgumentMatchers.argThat(instant -> instant.isAfter(NOW)));
    }

    /**
     * Perso il possesso durante la discovery, la giornata non viene nemmeno aperta.
     *
     * <p>Fermarsi qui non è questione di correttezza — la prima scrittura verrebbe comunque
     * rifiutata dalla chiusura — ma di lavoro buttato: proseguire significherebbe scaricare e
     * analizzare centinaia di megabyte per poi scoprire che non servivano a nulla.
     *
     * <p>E soprattutto l'esito <strong>non</strong> viene scritto: la giornata è di un altro
     * worker, e registrare qui un {@code WAITING_RETRY} sovrascriverebbe il suo stato facendolo
     * sembrare fermo in attesa.
     */
    @Test
    void persoIlPossessoDopoLaDiscoveryNonSiApreAlcunFile() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        // Il tempo passa oltre l'intervallo del battito, cosi' il rinnovo scatta davvero, e dice
        // che la giornata non e' piu' nostra.
        processor = new FdrDayProcessor(discovery, contentReader, fileReader, rowWriter,
                statusRepository, eventRepository, ownership, fileRepository,
                new FdrRetryPolicy(new FdrProperties()),
                Clock.fixed(NOW.plusSeconds(60), ZoneOffset.UTC));
        when(ownership.renew(any(), any())).thenReturn(false);

        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> processor.process(lease(), FdrDayScope.RESUME))
                .isInstanceOf(it.pagopa.cruscotto.ingestion.fdr.persistence.FdrLeaseLostException.class);

        verify(rowWriter, never()).openSession(any(FdrBlobFile.class), any());
        verify(statusRepository, never()).complete(anyLong(), any(), any(), any());
    }


    // ─── Possesso della giornata ─────────────────────────────────────────────────

    /**
     * Il possesso si rilascia <strong>una volta sola, dopo entrambi i dataset</strong>.
     *
     * <h2>Il difetto che questo test impedisce di reintrodurre</h2>
     *
     * <p>La prima stesura azzerava token e lease dentro {@code complete()}, cioè <em>per
     * dataset</em>. Sembrava prudente — l'esito e il rilascio nella stessa UPDATE — ed era
     * sbagliato di un livello: il possesso è della <em>giornata</em>. Appena il primo dataset
     * finiva, la sua riga perdeva il token; la chiusura transazionale del secondo trovava una riga
     * invece di due e concludeva di aver perso il possesso. In pratica <strong>nessuna giornata
     * poteva completare entrambi i dataset</strong>.
     *
     * <p>Nessun test con database finto poteva accorgersene: {@code renew} è mockato e risponde
     * sempre di sì. È uscito al primo giro reale contro dev — FLUSSI completo con 186.857 righe,
     * IUV morto al primo batch.
     */
    @Test
    void ilPossessoSiRilasciaUnaVoltaSolaDopoEntrambiIDataset() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        processor.process(lease(), FdrDayScope.RESUME);

        org.mockito.InOrder order = org.mockito.Mockito.inOrder(statusRepository);
        order.verify(statusRepository, times(2)).complete(anyLong(), any(), any(), any());
        order.verify(statusRepository).releaseOwnership(eq(BATCH), eq(DAY), eq("token-1"), any());
        verify(statusRepository, times(1))
                .releaseOwnership(anyLong(), any(), anyString(), any());
    }

    /**
     * La chiusura resta valida fino all'ultimo batch dell'ultimo dataset.
     *
     * <p>Qui il finto repository <strong>imita la semantica vera</strong>: il rinnovo riesce
     * finché il possesso non è stato rilasciato, e fallisce dopo. È l'unico modo per cui un test
     * con database finto possa vedere il difetto — con {@code renew} che risponde sempre di sì,
     * rilasciare a metà giornata non produce alcun sintomo.
     */
    @Test
    void laChiusuraRestaValidaFinoAllUltimoDataset() {
        java.util.concurrent.atomic.AtomicBoolean rilasciato =
                new java.util.concurrent.atomic.AtomicBoolean();
        when(statusRepository.releaseOwnership(anyLong(), any(), anyString(), any()))
                .thenAnswer(invocation -> {
                    rilasciato.set(true);
                    return 2;
                });
        org.mockito.Mockito.doAnswer(invocation -> !rilasciato.get())
                .when(ownership).renew(any(), any());

        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status())
                .as("se il possesso venisse rilasciato a meta' giornata, il secondo dataset "
                        + "morirebbe con una perdita di lease inventata")
                .isEqualTo(FdrProcessingStatus.COMPLETED);
        verify(rowWriter, times(2)).openSession(any(FdrBlobFile.class), any());
    }

    /**
     * Un dataset saltato dalla ripresa viene <strong>riportato al suo esito</strong>.
     *
     * <p>Il claim porta in {@code PROCESSING} entrambe le righe, anche quella di un dataset già
     * concluso. Saltarlo è giusto — rielaborarlo ne raddoppierebbe le righe — ma senza riscrivere
     * il suo stato la riga resterebbe in lavorazione per sempre, e il recupero la ritroverebbe a
     * ogni giro.
     *
     * <p>Si usa {@code restorePreviousOutcome} e non {@code complete} perché quest'ultimo
     * riscriverebbe anche i contatori, che nel risultato della ripresa sono a zero: cancellerebbe
     * i numeri veri del tentativo riuscito.
     */
    @Test
    void unDatasetSaltatoVieneRiportatoAlSuoEsitoSenzaToccareIContatori() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        processor.process(lease(FdrProcessingStatus.COMPLETED, FdrProcessingStatus.WAITING_RETRY),
                FdrDayScope.RESUME);

        verify(statusRepository).restorePreviousOutcome(eq(1L),
                eq(FdrProcessingStatus.COMPLETED), any());
        verify(statusRepository, never()).complete(eq(1L), any(), any(), any());
    }

    // ─── Percorso felice ─────────────────────────────────────────────────────────

    @Test
    void conEntrambiIDatasetCompletiLaGiornataECompletata() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0), write(100, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

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

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.businessDay()).isEqualTo(DAY);
        verify(discovery).discover(DAY);
    }

    // ─── Warning ─────────────────────────────────────────────────────────────────

    /**
     * Accorciare la causale è <strong>silenzioso</strong>: nessun evento, nessun avvertimento.
     *
     * <h2>Perché, e cosa asseriva prima questo test</h2>
     *
     * <p>Qui si verificava che 40.570 troncamenti producessero <em>un</em> evento aggregato invece
     * di 40.570 — l'aggregazione era giusta, la segnalazione no. Il cliente ha osservato che in
     * {@code FDR_INGESTION_EVENT} sembrava un errore, e ha chiesto di troncare in silenzio.
     *
     * <p>Ha ragione: accorciare {@code CAUSALE} a 30 caratteri è la trasformazione che lui stesso
     * ha chiesto, e su una giornata reale tocca <strong>una riga su cinque</strong>. Un
     * avvertimento che compare tutti i giorni non segnala più niente, e copre quelli veri.
     *
     * <p>Il numero non si perde: resta in {@code FDR_INGESTION_FILE.ROWS_TRUNCATED}, che è un
     * contatore e non una segnalazione.
     */
    @Test
    void quarantamilaTroncamentiNonProduconoNeEventoNeAvvertimento() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(186857, 186857, 0),
                write(186857, 0, new FdrTruncationSummary("CAUSALE", 40570, 35, 30, List.of())));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status())
                .as("una giornata in cui si accorciano le causali e' una giornata normale")
                .isEqualTo(FdrProcessingStatus.COMPLETED);
        verify(eventRepository, never()).record(anyLong(), any(), any(), any(), any(),
                anyString(), anyString(), eq("SOURCE_VALUE_TRUNCATED"), any(), any(), any());
    }

    /**
     * Il file vuoto invece resta un avvertimento: quello <em>è</em> un'anomalia della sorgente.
     *
     * <p>La distinzione è il punto dell'intera modifica. Non si è tolto il concetto di
     * avvertimento, si è tolto dall'elenco ciò che avvertimento non era.
     */
    @Test
    void ilFileVuotoRestaUnAvvertimento() {
        givenDiscovery(List.of(file(FdrDataset.FLUSSI_RENDICONTAZIONE),
                        file(FdrDataset.FLUSSI_RENDICONTAZIONE, 2)),
                List.of(file(FdrDataset.IUV_RENDICONTATI)));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class), any()))
                .thenAnswer(invocation -> session(write(0, 0, FdrTruncationSummary.none())))
                .thenAnswer(invocation -> session(write(100, 0, FdrTruncationSummary.none())))
                .thenAnswer(invocation -> session(write(10, 0, FdrTruncationSummary.none())));
        // Il primo dei tre file e' vuoto: header valido e zero righe dati.
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenReturn(read(0, 0, 0))
                .thenReturn(read(100, 100, 0))
                .thenReturn(read(10, 10, 0));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        verify(eventRepository, atLeastOnce()).record(anyLong(), any(), any(), any(), any(),
                eq(FdrEventType.EMPTY_SOURCE_FILE), any(), any(), any(), any(), any());
    }

    /**
     * La riga con la causale accorciata resta contata in N: è scritta, non fallita.
     *
     * <p>E la giornata resta <strong>{@code COMPLETED}</strong>: il valore è in banca dati, la
     * trasformazione è quella richiesta dal cliente, e non c'è nulla da segnalare.
     */
    @Test
    void laCausaleTroncataNonRiduceNeNeLEsito() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0),
                write(100, 0, new FdrTruncationSummary("CAUSALE", 21, 35, 30, List.of())));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.rowsProcessed()).isEqualTo(200);
        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).rowsFailed()).isZero();
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED);
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
        when(rowWriter.openSession(eq(pieno), any()))
                .thenAnswer(invocation -> session(write(100, 0, FdrTruncationSummary.none())));
        when(rowWriter.openSession(eq(vuoto), any()))
                .thenAnswer(invocation -> session(write(0, 0, FdrTruncationSummary.none())));
        when(rowWriter.openSession(eq(iuv), any()))
                .thenAnswer(invocation -> session(write(50, 0, FdrTruncationSummary.none())));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

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
        when(rowWriter.openSession(any(FdrBlobFile.class), any()))
                .thenAnswer(invocation -> session(write(95, 0, FdrTruncationSummary.none())));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).rowsFailed()).isEqualTo(5);
        // Il campione non finisce piu' in STG_INGEST_ERROR: l'evento aggregato porta conteggi,
        // categorie e campione, e resta UNO per file.
        verify(eventRepository, atLeastOnce()).record(anyLong(), any(), any(), any(), any(),
                eq(FdrEventType.ROWS_DISCARDED), any(), any(), any(), any(), any());
    }

    @Test
    void leRigheRifiutateDalDatabaseLascianoIlDatasetInWarning() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0), write(97, 3, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

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

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

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

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.datasets().get(FdrDataset.IUV_RENDICONTATI).status())
                .isEqualTo(FdrProcessingStatus.MISSING);
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
    }

    @Test
    void unDatasetConSoliFileVuotiFallisceSenzaRetry() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(0, 0, 0), write(0, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

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

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.WAITING_RETRY);
        verify(statusRepository, times(2)).complete(anyLong(), any(), eq(NOW),
                eq(NOW.plusSeconds(3600)));
    }

    /** Il container inesistente e' configurazione: nessun numero di tentativi lo crea. */
    @Test
    void unContainerInesistenteNonVieneRitentato() {
        when(discovery.discover(DAY)).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.CONTAINER_NOT_FOUND, "no container", null));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
    }

    @Test
    void unGuastoDelDatabaseMetteLaGiornataInAttesaDiRitentare() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenSessions();
        when(fileReader.read(any(), any(), any(), any()))
                .thenThrow(new DataAccessResourceFailureException("db down"));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.WAITING_RETRY);
    }

    /** Partizione mancante: configurazione, non guasto. Ritentare non la crea. */
    @Test
    void unaPartizioneMancanteFallisceSenzaRitentare() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class), any())).thenThrow(new FdrPersistenceException(
                FdrPersistenceException.Kind.MISSING_PARTITION, "no partition"));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

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

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

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

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

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
        when(discovery.discover(DAY)).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.LISTING_FAILED, "listing failed", null));

        FdrDayResult day = processor.process(leaseAtAttempt(5), FdrDayScope.RESUME);

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
        when(discovery.discover(DAY)).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.LISTING_FAILED, "listing failed", null));

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.FAILED);
        verifyNoRetryScheduled();
    }

    // ─── Idempotenza ─────────────────────────────────────────────────────────────

    /**
     * <strong>Il difetto che questo test impedisce.</strong> Una giornata puo' finire con i FLUSSI
     * completi e gli IUV in attesa di un nuovo tentativo: i due dataset si elaborano in sequenza e
     * il secondo puo' fallire da solo. L'unita' di ripresa resta pero' la <em>giornata</em>, quindi
     * senza questa regola il retry rielaborerebbe anche i FLUSSI — e la scrittura FDR e' un INSERT
     * fedele, senza deduplica: non li aggiornerebbe, li <em>raddoppierebbe</em>. In silenzio,
     * perche' X ed N tornerebbero comunque a combaciare.
     */
    @Test
    void inRipresaUnDatasetGiaConclusoNonVieneRielaborato() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(
                lease(FdrProcessingStatus.COMPLETED, FdrProcessingStatus.WAITING_RETRY),
                FdrDayScope.RESUME);

        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).status())
                .isEqualTo(FdrProcessingStatus.COMPLETED);
        // Una sola sessione di scrittura: quella degli IUV. Se ne aprisse due, i FLUSSI
        // verrebbero riscritti sopra se stessi.
        verify(rowWriter, times(1)).openSession(any(FdrBlobFile.class), any());
    }

    /**
     * Simmetrico, a parti invertite, e con l'altro stato terminale positivo.
     *
     * <p>{@code COMPLETED_WITH_WARNING} è terminale quanto {@code COMPLETED}: significa che le
     * righe sono in banca dati, con qualche scarto o qualche causale accorciata. Rielaborarlo
     * duplicherebbe esattamente come l'altro, quindi la regola non può guardare solo
     * {@code COMPLETED} — è il motivo per cui si appoggia a {@code isTerminalSuccess()} e non a un
     * confronto con una costante.
     */
    @Test
    void inRipresaValeAnchePerIlDatasetChiusoConAvvertimenti() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(
                lease(FdrProcessingStatus.WAITING_RETRY, FdrProcessingStatus.COMPLETED_WITH_WARNING),
                FdrDayScope.RESUME);

        assertThat(day.datasets().get(FdrDataset.IUV_RENDICONTATI).status())
                .isEqualTo(FdrProcessingStatus.COMPLETED_WITH_WARNING);
        verify(rowWriter, times(1)).openSession(any(FdrBlobFile.class), any());
    }

    /**
     * Un guasto di giornata <strong>non</strong> declassa un dataset già concluso.
     *
     * <p>Difetto reale, e il percorso per arrivarci è breve: una giornata metà conclusa che si
     * ritenta e trova Azure giù. La discovery fallisce per tutta la giornata, ed è corretto che i
     * FLUSSI non elaborati tornino in attesa — ma quelli <em>già scritti</em> non smettono di
     * esserci. Scrivergli sopra {@code WAITING_RETRY} con i contatori a zero farebbe credere al
     * tentativo successivo che siano da rifare, e la ripresa ne <strong>riscriverebbe</strong> le
     * righe: la scrittura è un INSERT fedele, senza deduplica, quindi 186.857 righe diventerebbero
     * 373.714 senza che un solo contatore lo segnali.
     *
     * <p>Il presidio è l'unico punto in cui i due percorsi devono coincidere: l'elaborazione
     * normale e il guasto di giornata saltano lo stesso dataset per la stessa ragione.
     */
    @Test
    void unGuastoDiGiornataNonDeclassaIlDatasetGiaConcluso() {
        when(discovery.discover(any())).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.LISTING_FAILED, "azure irraggiungibile", null));

        FdrDayResult day = processor.process(
                lease(FdrProcessingStatus.COMPLETED, FdrProcessingStatus.WAITING_RETRY),
                FdrDayScope.RESUME);

        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).status())
                .as("il dataset gia' in banca dati resta concluso")
                .isEqualTo(FdrProcessingStatus.COMPLETED);
        verify(statusRepository).restorePreviousOutcome(eq(1L),
                eq(FdrProcessingStatus.COMPLETED), any());
        verify(statusRepository, never()).complete(eq(1L), any(), any(), any());
        // Gli IUV invece tornano in attesa: di loro non si sa ancora nulla.
        assertThat(day.datasets().get(FdrDataset.IUV_RENDICONTATI).status())
                .isEqualTo(FdrProcessingStatus.WAITING_RETRY);
    }

    /**
     * Per la REINGESTION vale l'opposto, e il declassamento è corretto.
     *
     * <p>Le due partizioni sono già state svuotate prima di arrivare qui: quel {@code COMPLETED}
     * non descrive più nulla che esista in banca dati, e conservarlo farebbe risultare presente
     * una giornata che è stata cancellata.
     */
    @Test
    void inRicostruzioneUnGuastoDiGiornataDeclassaEntrambi() {
        when(discovery.discover(any())).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.LISTING_FAILED, "azure irraggiungibile", null));

        FdrDayResult day = processor.process(
                lease(FdrProcessingStatus.COMPLETED, FdrProcessingStatus.COMPLETED),
                FdrDayScope.REBUILD);

        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).status())
                .isEqualTo(FdrProcessingStatus.WAITING_RETRY);
        verify(statusRepository, never()).restorePreviousOutcome(anyLong(), any(), any());
    }

    /**
     * Un guasto nella scrittura dell'evento non cambia l'esito del dataset.
     *
     * <p><strong>Scenario reale, non ipotetico.</strong> Un'esecuzione contro un database passato
     * in sola lettura a metà giornata: il guasto vero era una connessione caduta — ritentabile —
     * ma la scrittura dell'evento falliva subito dopo con {@code SQLSTATE 25006}, e a risalire
     * all'orchestrazione era quest'ultima. Due danni: la classificazione passava a non ritentabile,
     * perché un errore inatteso lo è, e il dataset non riceveva alcun esito perché il bookkeeping
     * non veniva mai raggiunto.
     *
     * <p>Qui si verifica che il guasto dell'audit sia assorbito e che il dataset finisca comunque
     * in {@code WAITING_RETRY}, cioè che a decidere resti il guasto originale.
     */
    @Test
    void unGuastoNellaScritturaDellEventoNonCambiaLEsitoDelDataset() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(fileReader.read(any(), any(), any(FdrRowMapper.class), any(FdrBatchHandler.class)))
                .thenThrow(new DataAccessResourceFailureException("connessione caduta"));
        org.mockito.Mockito.doThrow(new DataAccessResourceFailureException("read-only"))
                .when(eventRepository).record(anyLong(), any(), any(), any(), any(), anyString(),
                        anyString(), anyString(), any(), any(), any());

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.datasets().get(FdrDataset.FLUSSI_RENDICONTAZIONE).status())
                .as("a decidere la ritentabilita' resta il guasto originale, non quello dell'audit")
                .isEqualTo(FdrProcessingStatus.WAITING_RETRY);
        verify(statusRepository, atLeastOnce()).complete(anyLong(), any(), any(), any());
    }

    /**
     * Lo stesso sul percorso del guasto di giornata, dove la discovery non arriva in fondo.
     *
     * <p>I due percorsi usano lo stesso presidio di proposito: erano due copie della stessa
     * decisione e sarebbero divergiti alla prima correzione fatta su una sola.
     */
    @Test
    void unGuastoNellaScritturaDellEventoNonBloccaLaChiusuraDellaGiornata() {
        when(discovery.discover(any())).thenThrow(new FdrStorageException(
                FdrStorageException.Kind.LISTING_FAILED, "azure irraggiungibile", null));
        org.mockito.Mockito.doThrow(new DataAccessResourceFailureException("read-only"))
                .when(eventRepository).record(anyLong(), any(), any(), any(), any(), anyString(),
                        anyString(), anyString(), any(), any(), any());

        FdrDayResult day = processor.process(lease(), FdrDayScope.RESUME);

        assertThat(day.status()).isEqualTo(FdrProcessingStatus.WAITING_RETRY);
        verify(statusRepository, atLeastOnce()).complete(anyLong(), any(), any(), any());
    }

    /**
     * Nel processore esiste <strong>un solo</strong> punto che scrive un evento, e sta dentro la
     * guardia.
     *
     * <p>I due test sopra coprono i percorsi di oggi; questo copre il prossimo. Aggiungere una
     * {@code eventRepository.record(...)} altrove è il modo naturale di reintrodurre il difetto —
     * un audit che fa cadere una giornata — e non farebbe fallire nessun altro test, perché
     * nessuno degli altri stuba quella scrittura per farla esplodere.
     *
     * <p>Si legge il sorgente e non il bytecode: la chiamata diretta e quella incapsulata hanno
     * la stessa forma a runtime, e la differenza che conta è testuale.
     */
    @Test
    void esisteUnSoloPuntoCheScriveEventi() throws java.io.IOException {
        String source = java.nio.file.Files.readString(java.nio.file.Path.of(
                "src/main/java/it/pagopa/cruscotto/ingestion/fdr/workflow/FdrDayProcessor.java"));
        long direct = source.lines().filter(line -> line.contains("eventRepository.record(")).count();

        assertThat(direct)
                .as("ogni evento deve passare da recordQuietly, che assorbe il guasto dell'audit "
                        + "invece di lasciarlo risalire al posto di quello vero")
                .isEqualTo(1);
        assertThat(source)
                .as("e quell'unica chiamata deve stare dentro la guardia")
                .contains("private RuntimeException recordQuietly(");
    }

    /** Nessuno dei due concluso: si elaborano entrambi, che è il caso normale. */
    @Test
    void inRipresaConEntrambiIncompletiSiElaboranoEntrambi() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        processor.process(
                lease(FdrProcessingStatus.WAITING_RETRY, FdrProcessingStatus.WAITING_RETRY),
                FdrDayScope.RESUME);

        verify(rowWriter, times(2)).openSession(any(FdrBlobFile.class), any());
    }

    /**
     * Entrambi già conclusi: non si scrive nulla.
     *
     * <p>In regime il claim non concederebbe nemmeno la giornata — gli stati terminali non sono
     * eleggibili — ma la regola vive comunque qui, e deve reggere da sola: è l'ultima rete prima di
     * una giornata raddoppiata, e non deve dipendere dal fatto che qualcun altro abbia filtrato
     * bene.
     */
    @Test
    void inRipresaConEntrambiGiaConclusiNonSiScriveNulla() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        FdrDayResult day = processor.process(
                lease(FdrProcessingStatus.COMPLETED, FdrProcessingStatus.COMPLETED),
                FdrDayScope.RESUME);

        verify(rowWriter, never()).openSession(any(FdrBlobFile.class), any());
        assertThat(day.status()).isEqualTo(FdrProcessingStatus.COMPLETED);
    }

    /**
     * La ripresa <strong>non</strong> salta i file: salta i dataset.
     *
     * <p>È una distinzione che vale la pena ancorare. Saltare un singolo blob perché il log dice
     * che era {@code COMPLETED} richiederebbe una garanzia di coerenza fra il commit del dato
     * business e il commit della riga di audit che non esiste — e dove non esiste, saltare un file
     * significa perderlo. Il log per file resta diagnostica.
     *
     * <p>Qui il dataset ripreso ha <em>due</em> file e li rilegge entrambi, senza mai interrogare
     * il repository del log.
     */
    @Test
    void laRipresaRileggeTuttiIFileDelDatasetSenzaConsultareIlLog() {
        givenDiscovery(List.of(file(FdrDataset.FLUSSI_RENDICONTAZIONE),
                        file(FdrDataset.FLUSSI_RENDICONTAZIONE, 2)),
                List.of(file(FdrDataset.IUV_RENDICONTATI)));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        processor.process(
                lease(FdrProcessingStatus.WAITING_RETRY, FdrProcessingStatus.COMPLETED),
                FdrDayScope.RESUME);

        verify(rowWriter, times(2))
                .openSession(any(FdrBlobFile.class), any());
        verify(fileRepository, times(2)).begin(anyLong(), any(), any(), anyInt(), any());
    }

    /**
     * La REINGESTION invece rifa' tutto, ed e' l'unica scelta corretta: le due partizioni sono
     * appena state svuotate dal truncate, quindi saltare un dataset perche' un tentativo precedente
     * lo aveva concluso lascerebbe la giornata <strong>senza</strong> quei dati — cancellati e mai
     * riscritti.
     */
    @Test
    void inRicostruzioneAncheUnDatasetGiaConclusoVieneRielaborato() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(10, 10, 0), write(10, 0, FdrTruncationSummary.none()));

        processor.process(lease(FdrProcessingStatus.COMPLETED, FdrProcessingStatus.COMPLETED),
                FdrDayScope.REBUILD);

        verify(rowWriter, times(2)).openSession(any(FdrBlobFile.class), any());
    }

    // ─── Stato persistito ────────────────────────────────────────────────────────

    @Test
    void loStatoPersistitoPortaXNEFallite() {
        givenDiscovery(file(FdrDataset.FLUSSI_RENDICONTAZIONE), file(FdrDataset.IUV_RENDICONTATI));
        givenFile(read(100, 100, 0), write(98, 2, FdrTruncationSummary.none()));

        processor.process(lease(), FdrDayScope.RESUME);

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

        processor.process(lease(), FdrDayScope.RESUME);

        
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
        when(rowWriter.openSession(any(FdrBlobFile.class), any()))
                .thenAnswer(invocation -> session(write(0, 0, FdrTruncationSummary.none())));
    }

    private void givenFile(FdrFileReadResult read, FdrWriteResult write) {
        when(contentReader.open(any())).thenReturn(new ByteArrayInputStream(new byte[0]));
        when(rowWriter.openSession(any(FdrBlobFile.class), any()))
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
                inserted + rejected, inserted, rejected, 1, 1, 0, truncation, Map.of(),
                List.of(), 10);
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
