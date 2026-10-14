package it.pagopa.cruscotto.ingestion.massivesearch.storage;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import it.pagopa.cruscotto.ingestion.configuration.AzureBlobProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Objects;

/**
 * Azure Blob Storage {@link MassiveSearchStorageService}, active for
 * {@code massive-search.storage.type=blob}.
 *
 * <p>Reuses the shared storage account of {@code cruscotto-backend}: the connection string is the
 * secret injected from the environment via {@code azure.blob.connection-string} (never hardcoded),
 * while the dedicated container comes from {@code massive-search.storage.container}. The logical
 * relative path is used verbatim as the blob name inside the container, mirroring the structure
 * {@code <container>/instances/{instanceId}/...} and {@code <container>/executions/{executionId}/...}.</p>
 *
 * <p>Only file references (blob paths) are ever persisted to the database; the file bytes live
 * exclusively on Blob Storage. Uploads and downloads are streamed, never fully buffered.</p>
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "massive-search.storage", name = "type", havingValue = "blob")
public class AzureBlobMassiveSearchStorageService implements MassiveSearchStorageService {

    private final BlobContainerClient containerClient;

    public AzureBlobMassiveSearchStorageService(
        AzureBlobProperties azureBlobProperties,
        MassiveSearchProperties massiveSearchProperties
    ) {
        String connectionString = azureBlobProperties.getConnectionString();
        String container = massiveSearchProperties.getStorage().getContainer();
        Objects.requireNonNull(connectionString, "azure.blob.connection-string is required for blob storage");
        Objects.requireNonNull(container, "massive-search.storage.container is required for blob storage");

        BlobServiceClient serviceClient = new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .buildClient();
        this.containerClient = serviceClient.getBlobContainerClient(container);
        this.containerClient.createIfNotExists();
    }

    @Override
    public StoredObject store(String relativePath, Charset charset, ContentWriter contentWriter) {
        String blobPath = normalize(relativePath);
        BlockBlobClient blob = containerClient.getBlobClient(blobPath).getBlockBlobClient();
        long rows;
        try (OutputStream out = blob.getBlobOutputStream(true);
             Writer writer = new OutputStreamWriter(out, charset)) {
            rows = contentWriter.writeTo(writer);
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to upload Massive Search file to blob " + blobPath, e);
        }
        logBlob("BLOB_UPLOAD_COMPLETED", "Uploaded file to blob storage", blobPath, "rows", rows);
        return new StoredObject(blobPath, rows);
    }

    @Override
    public StoredBinary storeBinary(String relativePath, BinaryWriter binaryWriter) {
        String blobPath = normalize(relativePath);
        BlockBlobClient blob = containerClient.getBlobClient(blobPath).getBlockBlobClient();
        long size;
        try (OutputStream out = blob.getBlobOutputStream(true);
             CountingOutputStream counting = new CountingOutputStream(out)) {
            binaryWriter.writeTo(counting);
            counting.flush();
            size = counting.getCount();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to upload Massive Search binary to blob " + blobPath, e);
        }
        logBlob("BLOB_UPLOAD_COMPLETED", "Uploaded binary to blob storage", blobPath, "size_bytes", size);
        return new StoredBinary(blobPath, size);
    }

    @Override
    public InputStream openForRead(String storagePath) {
        String blobPath = normalize(storagePath);
        BlobClient blob = containerClient.getBlobClient(blobPath);
        if (!blob.exists()) {
            throw new UncheckedIOException(new IOException("Blob not found: " + blobPath));
        }
        InputStream stream = blob.openInputStream();
        logBlob("BLOB_DOWNLOAD_COMPLETED", "Opened blob for read", blobPath, null, null);
        return stream;
    }

    @Override
    public void delete(String storagePath) {
        String blobPath = normalize(storagePath);
        boolean deleted = containerClient.getBlobClient(blobPath).deleteIfExists();
        logBlob("BLOB_DELETED", "Deleted blob", blobPath, "deleted", deleted);
    }

    /**
     * L'unica riga per operazione sul blob storage.
     *
     * <p>Le tre {@code BLOB_*_START} che precedevano upload e download sono state rimosse: erano
     * log di <em>intenzione</em>, e il documento chiede un solo log sintetico per confine di I/O,
     * quello con l'esito. {@code instanceId} ed {@code executionId} non si ripetono nel testo
     * perche' sono gia' attributi di primo livello, messi in MDC dal servizio di esecuzione.
     *
     * @param detailName {@code null} quando l'operazione non ha un contatore da riportare
     */
    private static void logBlob(String phase, String message, String blobPath,
                                String detailName, Object detailValue) {
        try (MdcScope ignored = MdcScope.phase(phase);
             LogDetails details = LogDetails.open()
                     .with(LogFields.DEPENDENCY, "azure-blob")
                     .with("blob_path", blobPath)
                     .with(detailName, detailValue)) {
            log.info(message);
        }
    }

    private String normalize(String path) {
        if (!StringUtils.hasText(path)) {
            throw new IllegalArgumentException("Blob path must not be blank");
        }
        String cleaned = path.replace('\\', '/');
        return cleaned.startsWith("/") ? cleaned.substring(1) : cleaned;
    }
}
