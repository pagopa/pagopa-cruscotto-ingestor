package it.pagopa.cruscotto.ingestion.fdr.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import it.pagopa.cruscotto.ingestion.configuration.AzureBlobProperties;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import org.springframework.util.StringUtils;

import java.util.Objects;

/**
 * Builds the container client the FDR services talk to.
 *
 * <p>Two services now reach the same container for two different reasons — one lists names, the
 * other opens streams — and the way the client is built carries a decision that must hold for both:
 * <strong>FDR never creates the container</strong>. Had each service built its own client, the day
 * someone adds {@code createIfNotExists()} to one of them the two would silently disagree, and the
 * cheaper of the two paths would be the one hiding the misconfiguration.
 *
 * <p>Package-private, and static: it is a constructor helper, not a bean. Making it a bean would
 * give the context something to inject that is neither configuration nor behaviour.
 */
final class FdrBlobContainerClients {

    private FdrBlobContainerClients() {
    }

    /**
     * @throws IllegalStateException if FDR is enabled without a connection string, which must stop
     *                               the application at startup rather than at the first run
     */
    static BlobContainerClient create(AzureBlobProperties azure, FdrProperties fdr) {
        String connectionString = azure.getConnectionString();
        String container = fdr.getStorage().getContainer();
        if (!StringUtils.hasText(connectionString)) {
            throw new IllegalStateException(
                    "azure.blob.connection-string is required when fdr.enabled is true");
        }
        Objects.requireNonNull(container, "fdr.storage.container is required when fdr.enabled is true");

        // No createIfNotExists(): FDR reads a container someone else fills, and creating a missing
        // one would turn a misconfiguration into a permanently empty day. See FdrStorageException.
        return new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .buildClient()
                .getBlobContainerClient(container);
    }
}
