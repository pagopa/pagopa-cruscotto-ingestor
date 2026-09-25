package it.pagopa.cruscotto.ingestion.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Shared Azure Blob Storage configuration, bound from the {@code azure.blob:} section.
 *
 * <p>Mirrors the modality already used by {@code cruscotto-backend}: the storage account is
 * addressed via a single connection string injected from the environment
 * ({@code AZURE_BLOB_CONNECTION_STRING}), never hardcoded. This is cross-cutting infrastructure:
 * the Massive Search feature reuses this same account (via its own dedicated container configured
 * under {@code massive-search.storage}) without duplicating the secret.</p>
 */
@Configuration
@ConfigurationProperties(prefix = "azure.blob")
@Getter
@Setter
public class AzureBlobProperties {

    /** Azure Storage account connection string (secret, injected from the environment). */
    private String connectionString;
}
