package it.pagopa.cruscotto.ingestion.fdr.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobInputStream;
import it.pagopa.cruscotto.ingestion.configuration.AzureBlobProperties;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.InputStream;
import java.math.BigInteger;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * L'apertura del contenuto di un blob, vista dal confine Azure.
 *
 * <p>Due cose contano qui, e nessuna delle due e' il percorso felice: che il file sparito fra la
 * discovery e la lettura sia distinguibile da un guasto di trasporto — perche' il primo si risolve
 * rileggendo la giornata e il secondo no — e che il contenuto non venga mai letto qui dentro.
 */
class FdrBlobContentReaderTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);
    private static final String CONTAINER = "fdr";
    private static final String BLOB = "20260915/IUV_RENDICONTATI_20260915-1.csv.gz";

    private final BlobContainerClient containerClient = mock(BlobContainerClient.class);
    private final BlobClient blobClient = mock(BlobClient.class);
    private final FdrBlobContentReader reader =
            new FdrBlobContentReader(containerClient, CONTAINER);

    @Test
    void apreLoStreamDelBlobRichiesto() {
        BlobInputStream expected = mock(BlobInputStream.class);
        when(containerClient.getBlobClient(BLOB)).thenReturn(blobClient);
        when(blobClient.exists()).thenReturn(true);
        when(blobClient.openInputStream()).thenReturn(expected);

        InputStream actual = reader.open(file());

        assertThat(actual).isSameAs(expected);
    }

    /**
     * Un blob elencato un istante prima e ora assente non e' un guasto: e' la cartella che e'
     * cambiata sotto. Dirlo con un tipo diverso e' cio' che permette al retry di distinguere fra
     * "rileggi la giornata" e "richiedi di nuovo lo stesso file ad Azure".
     */
    @Test
    void unBlobSparitoDopoLaDiscoveryHaUnEsitoSuo() {
        when(containerClient.getBlobClient(BLOB)).thenReturn(blobClient);
        when(blobClient.exists()).thenReturn(false);

        assertThatThrownBy(() -> reader.open(file()))
                .isInstanceOf(FdrStorageException.class)
                .hasMessageContaining(BLOB)
                .extracting(e -> ((FdrStorageException) e).kind())
                .isEqualTo(FdrStorageException.Kind.BLOB_NOT_FOUND);
    }

    @Test
    void unQuattrocentoQuattroDiAzureEUnBlobNonTrovato() {
        BlobStorageException notFound = mock(BlobStorageException.class);
        when(notFound.getStatusCode()).thenReturn(404);
        when(containerClient.getBlobClient(BLOB)).thenReturn(blobClient);
        when(blobClient.exists()).thenThrow(notFound);

        assertThatThrownBy(() -> reader.open(file()))
                .isInstanceOf(FdrStorageException.class)
                .extracting(e -> ((FdrStorageException) e).kind())
                .isEqualTo(FdrStorageException.Kind.BLOB_NOT_FOUND);
    }

    @Test
    void unAltroErroreDiAzureEUnFallimentoDiDownload() {
        BlobStorageException forbidden = mock(BlobStorageException.class);
        when(forbidden.getStatusCode()).thenReturn(403);
        when(containerClient.getBlobClient(BLOB)).thenReturn(blobClient);
        when(blobClient.exists()).thenReturn(true);
        when(blobClient.openInputStream()).thenThrow(forbidden);

        assertThatThrownBy(() -> reader.open(file()))
                .isInstanceOf(FdrStorageException.class)
                .extracting(e -> ((FdrStorageException) e).kind())
                .isEqualTo(FdrStorageException.Kind.DOWNLOAD_FAILED);
    }

    /**
     * Qualunque guasto non previsto dell'SDK resta un fallimento classificato: lasciarlo passare
     * nudo significherebbe che il chiamante non puo' decidere se ritentare.
     */
    @Test
    void unGuastoImprevistoDellSdkRestaClassificato() {
        when(containerClient.getBlobClient(BLOB)).thenReturn(blobClient);
        when(blobClient.exists()).thenThrow(new IllegalStateException("client closed"));

        assertThatThrownBy(() -> reader.open(file()))
                .isInstanceOf(FdrStorageException.class)
                .extracting(e -> ((FdrStorageException) e).kind())
                .isEqualTo(FdrStorageException.Kind.DOWNLOAD_FAILED);
    }

    // ─── Feature flag ────────────────────────────────────────────────────────────

    /**
     * Senza il flag il bean non esiste: il costruttore cercherebbe una connection string che gli
     * ambienti non-FDR non hanno motivo di fornire, trasformando una feature spenta in un avvio
     * fallito.
     */
    @Test
    void ilBeanNonEsisteSenzaIlFeatureFlag() {
        new ApplicationContextRunner()
                .withUserConfiguration(FdrBlobContentReader.class)
                .withBean(AzureBlobProperties.class)
                .withBean(FdrProperties.class)
                .withPropertyValues("fdr.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(FdrBlobContentReader.class));
    }

    private static FdrBlobFile file() {
        return new FdrBlobFile(BLOB, FdrDataset.IUV_RENDICONTATI, DAY, BigInteger.ONE,
                1_024L, "\"0x1\"", null);
    }
}
