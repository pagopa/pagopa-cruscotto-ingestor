package it.pagopa.cruscotto.ingestion.massivesearch.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import it.pagopa.cruscotto.ingestion.configuration.AzureBlobProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
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
        log.info("phase=BLOB_UPLOAD_START instanceId={} executionId={} blobPath={}",
            MDC.get("instanceId"), MDC.get("executionId"), blobPath);
        long rows;
        try (OutputStream out = blob.getBlobOutputStream(true);
             Writer writer = new OutputStreamWriter(out, charset)) {
            rows = contentWriter.writeTo(writer);
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to upload Massive Search file to blob " + blobPath, e);
        }
        log.info("phase=BLOB_UPLOAD_COMPLETED instanceId={} executionId={} blobPath={} rows={}",
            MDC.get("instanceId"), MDC.get("executionId"), blobPath, rows);
        return new StoredObject(blobPath, rows);
    }

    @Override
    public StoredBinary storeBinary(String relativePath, BinaryWriter binaryWriter) {
        String blobPath = normalize(relativePath);
        BlockBlobClient blob = containerClient.getBlobClient(blobPath).getBlockBlobClient();
        log.info("phase=BLOB_UPLOAD_START instanceId={} executionId={} blobPath={}",
            MDC.get("instanceId"), MDC.get("executionId"), blobPath);
        long size;
        try (OutputStream out = blob.getBlobOutputStream(true);
             CountingOutputStream counting = new CountingOutputStream(out)) {
            binaryWriter.writeTo(counting);
            counting.flush();
            size = counting.getCount();
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to upload Massive Search binary to blob " + blobPath, e);
        }
        log.info("phase=BLOB_UPLOAD_COMPLETED instanceId={} executionId={} blobPath={} sizeBytes={}",
            MDC.get("instanceId"), MDC.get("executionId"), blobPath, size);
        return new StoredBinary(blobPath, size);
    }

    @Override
    public InputStream openForRead(String storagePath) {
        String blobPath = normalize(storagePath);
        BlobClient blob = containerClient.getBlobClient(blobPath);
        log.info("phase=BLOB_DOWNLOAD_START instanceId={} executionId={} blobPath={}",
            MDC.get("instanceId"), MDC.get("executionId"), blobPath);
        if (!blob.exists()) {
            throw new UncheckedIOException(new IOException("Blob not found: " + blobPath));
        }
        InputStream stream = blob.openInputStream();
        log.info("phase=BLOB_DOWNLOAD_COMPLETED instanceId={} executionId={} blobPath={}",
            MDC.get("instanceId"), MDC.get("executionId"), blobPath);
        return stream;
    }

    @Override
    public void delete(String storagePath) {
        String blobPath = normalize(storagePath);
        boolean deleted = containerClient.getBlobClient(blobPath).deleteIfExists();
        log.info("phase=BLOB_DELETED instanceId={} executionId={} blobPath={} deleted={}",
            MDC.get("instanceId"), MDC.get("executionId"), blobPath, deleted);
    }

    private String normalize(String path) {
        if (!StringUtils.hasText(path)) {
            throw new IllegalArgumentException("Blob path must not be blank");
        }
        String cleaned = path.replace('\\', '/');
        return cleaned.startsWith("/") ? cleaned.substring(1) : cleaned;
    }
}
