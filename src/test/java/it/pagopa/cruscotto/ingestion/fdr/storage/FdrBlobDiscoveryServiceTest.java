package it.pagopa.cruscotto.ingestion.fdr.storage;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import it.pagopa.cruscotto.ingestion.configuration.AzureBlobProperties;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * La discovery vista dal confine Azure: cosa viene chiesto al client, e cosa esce dall'altra parte.
 *
 * <p>Il client Azure e' stubato tramite il costruttore package-private, che e' il confine piu'
 * piccolo possibile: avvolgere l'SDK in un'astrazione nostra sarebbe stato molto codice da
 * mantenere per nessun comportamento in piu'. Le regole sul nome file stanno nel test del parser;
 * qui si verifica l'elenco, il raggruppamento, l'ordinamento, la mappatura dei metadata e — la
 * garanzia piu' importante — che il contenuto non venga mai aperto.
 */
class FdrBlobDiscoveryServiceTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final String CONTAINER = "fdr";

    private final BlobContainerClient containerClient = mock(BlobContainerClient.class);
    private final FdrBlobDiscoveryService service = new FdrBlobDiscoveryService(containerClient, CONTAINER);

    // ─── Interrogazione di Azure ─────────────────────────────────────────────────

    /**
     * La giornata si traduce in un prefisso, e il prefisso finisce davvero nella richiesta: senza,
     * il listing tornerebbe l'intero container e la discovery di un giorno leggerebbe tutti gli
     * altri.
     */
    @Test
    void ilListingChiedeSoloLaCartellaDellaGiornata() {
        givenBlobs();

        service.discover(DAY);

        ArgumentCaptor<ListBlobsOptions> options = ArgumentCaptor.forClass(ListBlobsOptions.class);
        verify(containerClient).listBlobs(options.capture(), any());
        assertThat(options.getValue().getPrefix()).isEqualTo("20260915/");
    }

    /**
     * LA garanzia di questo step. La discovery lavora sui soli metadata: se aprisse i blob, una
     * giornata con centinaia di file da ~400 MB verrebbe scaricata per sapere quanti file ci sono.
     * Il test guarda il client, che e' l'unica via per arrivare al contenuto.
     */
    @Test
    void laDiscoveryNonApreMaiIlContenutoDeiBlob() {
        givenBlobs(blob("20260915/IUV_RENDICONTATI_20260915-1.csv.gz", 400_000_000L));

        service.discover(DAY);

        verify(containerClient, never()).getBlobClient(any());
        verify(containerClient, never()).getBlobVersionClient(any(), any());
    }

    // ─── Raggruppamento, ordinamento, metadata ───────────────────────────────────

    /**
     * L'ordinamento usa il valore numerico. Come testo, {@code -10} precederebbe {@code -2} e i
     * file verrebbero elaborati in un ordine in cui il cliente non li ha mai numerati.
     */
    @Test
    void iFileSonoOrdinatiPerProgressivoNumericoNonAlfabetico() {
        givenBlobs(
                blob("20260915/IUV_RENDICONTATI_20260915-10.csv.gz"),
                blob("20260915/IUV_RENDICONTATI_20260915-2.csv.gz"),
                blob("20260915/IUV_RENDICONTATI_20260915-1.csv.gz"));

        FdrDayDiscovery discovery = service.discover(DAY);

        assertThat(discovery.filesOf(FdrDataset.IUV_RENDICONTATI))
                .extracting(FdrBlobFile::progressive)
                .containsExactly(BigInteger.ONE, BigInteger.TWO, BigInteger.TEN);
    }

    /**
     * L'ordinamento guarda il <strong>valore</strong>, non la scrittura: {@code -002} vale 2 e sta
     * fra {@code -1} e {@code -10}. Ordinati come testo l'esito sarebbe 1, 10, 002 — tre file
     * elaborati in un ordine che il cliente non ha mai numerato.
     */
    @Test
    void gliZeriInizialiNonAlteranoLOrdinamento() {
        givenBlobs(
                blob("20260915/IUV_RENDICONTATI_20260915-10.csv.gz"),
                blob("20260915/IUV_RENDICONTATI_20260915-002.csv.gz"),
                blob("20260915/IUV_RENDICONTATI_20260915-1.csv.gz"));

        assertThat(service.discover(DAY).filesOf(FdrDataset.IUV_RENDICONTATI))
                .extracting(FdrBlobFile::blobName)
                .containsExactly(
                        "20260915/IUV_RENDICONTATI_20260915-1.csv.gz",
                        "20260915/IUV_RENDICONTATI_20260915-002.csv.gz",
                        "20260915/IUV_RENDICONTATI_20260915-10.csv.gz");
    }

    /**
     * Due scritture dello stesso valore sono due blob, e vanno restituiti entrambi. Deduplicarli, o
     * indicizzarli in una mappa per progressivo, ne perderebbe uno in silenzio: la discovery non
     * deve mai far sparire un file che esiste davvero.
     *
     * <p>A ordine uguale decide il nome, cosi' il risultato resta deterministico e due esecuzioni
     * sulla stessa cartella non possono discordare. Se avere due rappresentazioni dello stesso
     * numero sia un'anomalia da segnalare lo dira' l'audit; qui non si perde nulla.
     */
    @Test
    void dueScrittureDelloStessoProgressivoRestanoDueFile() {
        givenBlobs(
                blob("20260915/IUV_RENDICONTATI_20260915-001.csv.gz"),
                blob("20260915/IUV_RENDICONTATI_20260915-1.csv.gz"));

        List<FdrBlobFile> files = service.discover(DAY).filesOf(FdrDataset.IUV_RENDICONTATI);

        assertThat(files).hasSize(2);
        assertThat(files).extracting(FdrBlobFile::progressive)
                .containsExactly(BigInteger.ONE, BigInteger.ONE);
        assertThat(files).extracting(FdrBlobFile::blobName)
                .as("a progressivo uguale l'ordine lo decide il nome")
                .containsExactly(
                        "20260915/IUV_RENDICONTATI_20260915-001.csv.gz",
                        "20260915/IUV_RENDICONTATI_20260915-1.csv.gz");
    }

    /**
     * Un progressivo oltre la capacita' di {@code long} e' un file come gli altri: viene restituito
     * e si ordina al posto giusto. Con un tipo primitivo sarebbe diventato un errore tecnico che
     * avrebbe fatto fallire l'intera giornata per un nome.
     */
    @Test
    void unProgressivoEnormeVieneRestituitoEordinatoInFondo() {
        givenBlobs(
                blob("20260915/IUV_RENDICONTATI_20260915-99999999999999999999999.csv.gz"),
                blob("20260915/IUV_RENDICONTATI_20260915-2.csv.gz"));

        assertThat(service.discover(DAY).filesOf(FdrDataset.IUV_RENDICONTATI))
                .extracting(FdrBlobFile::progressive)
                .containsExactly(BigInteger.TWO, new BigInteger("99999999999999999999999"));
    }

    @Test
    void iDueDatasetSonoRaggruppatiSeparatamenteECiascunoOrdinato() {
        givenBlobs(
                blob("20260915/IUV_RENDICONTATI_20260915-3.csv.gz"),
                blob("20260915/FLUSSI_RENDICONTAZIONE_20260915-2.csv.gz"),
                blob("20260915/IUV_RENDICONTATI_20260915-1.csv.gz"),
                blob("20260915/FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz"));

        FdrDayDiscovery discovery = service.discover(DAY);

        assertThat(discovery.filesOf(FdrDataset.FLUSSI_RENDICONTAZIONE))
                .extracting(FdrBlobFile::progressive).containsExactly(BigInteger.ONE, BigInteger.TWO);
        assertThat(discovery.filesOf(FdrDataset.IUV_RENDICONTATI))
                .extracting(FdrBlobFile::progressive).containsExactly(BigInteger.ONE, BigInteger.valueOf(3));
        assertThat(discovery.validCount()).isEqualTo(4);
    }

    @Test
    void iMetadataAzureVengonoRiportatiSulModello() {
        OffsetDateTime modified = OffsetDateTime.of(2026, 9, 15, 6, 30, 0, 0, ZoneOffset.UTC);
        givenBlobs(blob("20260915/FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz", 123_456L, "0x8DABC", modified));

        FdrBlobFile file = service.discover(DAY).filesOf(FdrDataset.FLUSSI_RENDICONTAZIONE).get(0);

        assertThat(file.blobName()).isEqualTo("20260915/FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz");
        assertThat(file.dataset()).isEqualTo(FdrDataset.FLUSSI_RENDICONTAZIONE);
        assertThat(file.businessDay()).isEqualTo(DAY);
        assertThat(file.progressive()).isEqualTo(BigInteger.ONE);
        assertThat(file.sizeBytes()).isEqualTo(123_456L);
        assertThat(file.eTag()).isEqualTo("0x8DABC");
        assertThat(file.lastModified()).isEqualTo(modified);
    }

    /**
     * Azure puo' non riportare le properties. Un metadata mancante non deve far saltare la
     * giornata: il file resta elaborabile, la dimensione diventa {@code -1} e si vede che non e'
     * nota invece di essere confusa con uno zero.
     */
    @Test
    void unFileSenzaPropertiesRestaElaborabile() {
        BlobItem bare = new BlobItem();
        bare.setName("20260915/IUV_RENDICONTATI_20260915-1.csv.gz");
        givenBlobs(bare);

        FdrBlobFile file = service.discover(DAY).filesOf(FdrDataset.IUV_RENDICONTATI).get(0);

        assertThat(file.sizeBytes()).isEqualTo(-1L);
        assertThat(file.eTag()).isNull();
        assertThat(file.lastModified()).isNull();
    }

    // ─── Giornate parziali e vuote ───────────────────────────────────────────────

    /**
     * Ogni dataset e' sempre una chiave. "FLUSSI 1, IUV 0" si legge come un fatto, senza controlli
     * sul null, ed e' la condizione che l'orchestrazione dovra' valutare: qui non diventa un errore.
     */
    @Test
    void unaGiornataConUnSoloDatasetRiportaZeroSullAltro() {
        givenBlobs(blob("20260915/FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz"));

        FdrDayDiscovery discovery = service.discover(DAY);

        assertThat(discovery.countOf(FdrDataset.FLUSSI_RENDICONTAZIONE)).isEqualTo(1);
        assertThat(discovery.countOf(FdrDataset.IUV_RENDICONTATI)).isZero();
        assertThat(discovery.filesOf(FdrDataset.IUV_RENDICONTATI)).isEmpty();
        assertThat(discovery.isEmpty()).isFalse();
    }

    @Test
    void unaGiornataConIlSoloIuvRiportaZeroSuiFlussi() {
        givenBlobs(blob("20260915/IUV_RENDICONTATI_20260915-1.csv.gz"));

        FdrDayDiscovery discovery = service.discover(DAY);

        assertThat(discovery.countOf(FdrDataset.IUV_RENDICONTATI)).isEqualTo(1);
        assertThat(discovery.countOf(FdrDataset.FLUSSI_RENDICONTAZIONE)).isZero();
    }

    @Test
    void unaGiornataSenzaFileEunRisultatoValidoEvuoto() {
        givenBlobs();

        FdrDayDiscovery discovery = service.discover(DAY);

        assertThat(discovery.isEmpty()).isTrue();
        assertThat(discovery.validCount()).isZero();
        assertThat(discovery.invalidEntries()).isEmpty();
        assertThat(discovery.businessDay()).isEqualTo(DAY);
        assertThat(discovery.prefix()).isEqualTo("20260915/");
        assertThat(discovery.container()).isEqualTo(CONTAINER);
    }

    /**
     * Una cartella piena di file non riconoscibili e' vuota quanto una cartella vuota, ma per una
     * ragione opposta: le anomalie classificate sono cio' che permette di distinguerle senza
     * andare a guardare il container.
     */
    @Test
    void leAnomalieSonoClassificateSenzaFarFallireLaGiornata() {
        givenBlobs(
                blob("20260915/IUV_RENDICONTATI_20260916-1.csv.gz"),   // giorno sbagliato
                blob("20260915/ALTRO_20260915-1.csv.gz"),              // dataset sconosciuto
                blob("20260915/IUV_RENDICONTATI_20260915-1.zip"),      // estensione
                blob("20260915/IUV_RENDICONTATI_20260915-X.csv.gz"),   // progressivo
                blob("20260915/IUV_RENDICONTATI_20260915-1.csv.gz"));  // valido

        FdrDayDiscovery discovery = service.discover(DAY);

        assertThat(discovery.validCount()).isEqualTo(1);
        assertThat(discovery.invalidEntries())
                .extracting(FdrInvalidEntry::reason)
                .containsExactlyInAnyOrder(
                        FdrInvalidEntry.Reason.WRONG_DAY,
                        FdrInvalidEntry.Reason.UNKNOWN_DATASET,
                        FdrInvalidEntry.Reason.WRONG_EXTENSION,
                        FdrInvalidEntry.Reason.INVALID_PROGRESSIVE);
    }

    /** I marcatori di cartella non sono file: vanno ignorati, non contati come anomalie. */
    @Test
    void iMarcatoriDiCartellaVengonoIgnoratiSenzaFinireFraLeAnomalie() {
        BlobItem folderMarker = new BlobItem();
        folderMarker.setName("20260915/");
        BlobItem virtualPrefix = new BlobItem();
        virtualPrefix.setName("20260915/sottocartella/");
        virtualPrefix.setIsPrefix(true);
        givenBlobs(folderMarker, virtualPrefix, blob("20260915/IUV_RENDICONTATI_20260915-1.csv.gz"));

        FdrDayDiscovery discovery = service.discover(DAY);

        assertThat(discovery.validCount()).isEqualTo(1);
        assertThat(discovery.invalidEntries()).isEmpty();
    }

    // ─── Errori Azure ────────────────────────────────────────────────────────────

    /**
     * La distinzione che giustifica l'eccezione. Un container inesistente che tornasse "nessun
     * file" farebbe registrare la giornata come elaborata senza aver letto nulla, e l'errore di
     * configurazione resterebbe invisibile finche' qualcuno non cercasse i dati.
     */
    @Test
    void unContainerInesistenteNonEunaGiornataVuota() {
        BlobStorageException notFound = mock(BlobStorageException.class);
        when(notFound.getStatusCode()).thenReturn(404);
        when(containerClient.listBlobs(any(), any())).thenThrow(notFound);

        assertThatThrownBy(() -> service.discover(DAY))
                .isInstanceOf(FdrStorageException.class)
                .hasMessageContaining(CONTAINER)
                .extracting(e -> ((FdrStorageException) e).kind())
                .isEqualTo(FdrStorageException.Kind.CONTAINER_NOT_FOUND);
    }

    /**
     * Gli altri errori Azure restano distinti dal container mancante: il primo non si risolve
     * ritentando, gli altri si', e il futuro retry dovra' poterli separare.
     */
    @Test
    void unErroreAzureGenericoEclassificatoComeFallimentoDelListing() {
        BlobStorageException forbidden = mock(BlobStorageException.class);
        when(forbidden.getStatusCode()).thenReturn(403);
        when(containerClient.listBlobs(any(), any())).thenThrow(forbidden);

        assertThatThrownBy(() -> service.discover(DAY))
                .isInstanceOf(FdrStorageException.class)
                .extracting(e -> ((FdrStorageException) e).kind())
                .isEqualTo(FdrStorageException.Kind.LISTING_FAILED);
    }

    /** Anche un guasto che non arriva dall'SDK non deve sfuggire come eccezione generica. */
    @Test
    void unGuastoNonAzureVieneComunqueIncapsulato() {
        when(containerClient.listBlobs(any(), any()))
                .thenThrow(new IllegalStateException("connessione interrotta"));

        assertThatThrownBy(() -> service.discover(DAY))
                .isInstanceOf(FdrStorageException.class)
                .hasRootCauseMessage("connessione interrotta");
    }

    // ─── Container configurato ───────────────────────────────────────────────────

    /** Il container custom deve arrivare davvero al risultato, non solo alla configurazione. */
    @Test
    void ilContainerConfiguratoEquelloRiportatoNelRisultato() {
        FdrBlobDiscoveryService custom = new FdrBlobDiscoveryService(containerClient, "fdr-uat");
        givenBlobs();

        assertThat(custom.discover(DAY).container()).isEqualTo("fdr-uat");
    }

    // ─── Esistenza del bean ──────────────────────────────────────────────────────

    /**
     * La garanzia che tiene in piedi tutto il resto. Questo servizio costruisce un client Azure nel
     * costruttore: senza la condizione sul flag verrebbe istanziato in <strong>ogni</strong>
     * ambiente, anche in quelli che non sanno nulla di FDR e non hanno motivo di fornire una
     * connection string, e l'applicazione non partirebbe. Una feature dormiente diventerebbe un
     * avvio fallito.
     *
     * <p>Il caso senza alcuna property e' quello reale degli ambienti esistenti: il default e'
     * spento, quindi il bean non deve nemmeno essere creato.
     */
    @Test
    void senzaConfigurazioneFdrIlServizioNonVieneCreato() {
        new ApplicationContextRunner()
                .withUserConfiguration(FdrBlobDiscoveryService.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(FdrBlobDiscoveryService.class);
                });
    }

    @Test
    void aFeatureEsplicitamenteSpentaIlServizioNonVieneCreato() {
        new ApplicationContextRunner()
                .withUserConfiguration(FdrBlobDiscoveryService.class)
                .withPropertyValues("fdr.enabled=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(FdrBlobDiscoveryService.class);
                });
    }

    /**
     * Acceso ma senza il segreto dello storage, l'avvio deve fermarsi subito e dire quale property
     * manca. L'alternativa sarebbe un client costruito a meta' che fallisce alla prima lettura,
     * quando la causa e' ormai lontana.
     */
    @Test
    void aFeatureAccesaSenzaConnectionStringLAvvioSiFermaSubito() {
        AzureBlobProperties senzaSegreto = new AzureBlobProperties();
        FdrProperties fdr = new FdrProperties();

        assertThatThrownBy(() -> new FdrBlobDiscoveryService(senzaSegreto, fdr))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("azure.blob.connection-string")
                .hasMessageContaining("fdr.enabled");
    }

    // ─── Utilita' ────────────────────────────────────────────────────────────────

    /**
     * Consegna al client stubato l'elenco dato.
     *
     * <p>Il {@code PagedIterable} dell'SDK percorre da solo tutte le pagine man mano che lo si
     * itera: qui viene stubato il suo iteratore, quindi il test dimostra che il servizio consuma
     * l'intero iterabile e non si ferma a cio' che ha visto per primo.
     */
    @SuppressWarnings("unchecked")
    private void givenBlobs(BlobItem... items) {
        PagedIterable<BlobItem> paged = mock(PagedIterable.class);
        List<BlobItem> all = Arrays.asList(items);
        when(paged.iterator()).thenReturn(all.iterator());
        when(containerClient.listBlobs(any(ListBlobsOptions.class), any())).thenReturn(paged);
    }

    private static BlobItem blob(String name) {
        return blob(name, 1_024L);
    }

    private static BlobItem blob(String name, long length) {
        return blob(name, length, "0xETAG", OffsetDateTime.now(ZoneOffset.UTC));
    }

    private static BlobItem blob(String name, long length, String eTag, OffsetDateTime modified) {
        BlobItemProperties properties = new BlobItemProperties()
                .setContentLength(length)
                .setETag(eTag)
                .setLastModified(modified);
        BlobItem item = new BlobItem();
        item.setName(name);
        item.setProperties(properties);
        return item;
    }
}
