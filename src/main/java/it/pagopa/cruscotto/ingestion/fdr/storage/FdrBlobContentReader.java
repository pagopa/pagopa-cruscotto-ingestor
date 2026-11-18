package it.pagopa.cruscotto.ingestion.fdr.storage;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import it.pagopa.cruscotto.ingestion.configuration.AzureBlobProperties;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Objects;

/**
 * Opens the content of one FDR blob as a stream.
 *
 * <p>Separate from {@link FdrBlobDiscoveryService} because the two do genuinely different things:
 * discovery answers "what is there" over a whole day and is cheap enough to run on a day that turns
 * out to be empty, while this opens a single file that can reach 400 MB. Keeping them apart also
 * keeps the discovery test free of streams and this one free of listings.
 *
 * <p><strong>It returns the stream, it does not read it.</strong> Nothing here decompresses,
 * decodes or counts: the caller owns the stream and must close it. That is what lets the reading
 * side be tested on a byte array with no Azure in sight, and it is the only reason a 400 MB file
 * never has to exist in memory — the bytes flow from the socket to the parser without an
 * intermediate buffer of our making.
 *
 * <p>The {@code exists()} probe is deliberate even though {@code openInputStream()} would fail on
 * its own: the failure would arrive as a generic storage exception and the day would be recorded as
 * a technical fault, when the actual event — a file listed a moment ago and now gone — is the
 * folder changing underneath us. See {@link FdrStorageException.Kind#BLOB_NOT_FOUND}. It costs one
 * HEAD request per file, against a file whose download is about to cost orders of magnitude more.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrBlobContentReader {

    private final BlobContainerClient containerClient;
    private final String container;

    public FdrBlobContentReader(AzureBlobProperties azureBlobProperties, FdrProperties fdrProperties) {
        this(FdrBlobContainerClients.create(azureBlobProperties, fdrProperties),
                fdrProperties.getStorage().getContainer());
    }

    /** Seam for the tests, which supply a stubbed container client. See the discovery service. */
    FdrBlobContentReader(BlobContainerClient containerClient, String container) {
        this.containerClient = containerClient;
        this.container = container;
    }

    /**
     * Opens the compressed content of a file previously returned by discovery.
     *
     * @param file the file to read; only its {@link FdrBlobFile#blobName()} is used, the rest is
     *             metadata captured at discovery time and may already be stale
     * @return the raw, still compressed stream. The caller closes it.
     * @throws FdrStorageException with {@link FdrStorageException.Kind#BLOB_NOT_FOUND} when the blob
     *                             has disappeared since discovery, {@code DOWNLOAD_FAILED} otherwise
     */
    public InputStream open(FdrBlobFile file) {
        Objects.requireNonNull(file, "file");
        String blobName = Objects.requireNonNull(file.blobName(), "file.blobName");

        try {
            BlobClient blob = containerClient.getBlobClient(blobName);
            if (!blob.exists()) {
                throw new FdrStorageException(FdrStorageException.Kind.BLOB_NOT_FOUND,
                        "FDR blob " + blobName + " no longer exists in container " + container, null);
            }
            return blob.openInputStream();
        } catch (BlobStorageException e) {
            throw new FdrStorageException(kindOf(e),
                    "Unable to open FDR blob " + blobName + " in container " + container, e);
        } catch (FdrStorageException e) {
            // Already classified just above: rethrown untouched so that the generic RuntimeException
            // branch below does not downgrade a precise reason into DOWNLOAD_FAILED.
            throw e;
        } catch (RuntimeException e) {
            throw new FdrStorageException(FdrStorageException.Kind.DOWNLOAD_FAILED,
                    "Unable to open FDR blob " + blobName + " in container " + container, e);
        }
    }

    private static FdrStorageException.Kind kindOf(BlobStorageException e) {
        return e.getStatusCode() == 404
                ? FdrStorageException.Kind.BLOB_NOT_FOUND
                : FdrStorageException.Kind.DOWNLOAD_FAILED;
    }
}
