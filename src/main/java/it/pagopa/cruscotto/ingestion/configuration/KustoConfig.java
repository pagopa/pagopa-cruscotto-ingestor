package it.pagopa.cruscotto.ingestion.configuration;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KustoConfig {

    @Value("${azure.kusto.cluster.url}")
    private String clusterUrl;

    @Value("${azure.kusto.app.id}")
    private String appId;

    @Value("${azure.kusto.app.key}")
    private String appKey;

    @Value("${azure.kusto.tenant.id}")
    private String tenantId;

    @Bean
    public Client kustoClient() throws Exception {
        validateCredentials();
        ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                clusterUrl, appId, appKey, tenantId);
        return ClientFactory.createClient(kcsb);
    }

    private void validateCredentials() {
        requireValue(clusterUrl, "azure.kusto.cluster.url");
        requireValue(appId, "azure.kusto.app.id");
        requireValue(appKey, "azure.kusto.app.key");
        requireValue(tenantId, "azure.kusto.tenant.id");

        if (looksLikeSecretId(appKey)) {
            throw new IllegalStateException(
                    "Invalid azure.kusto.app.key: expected the client secret value, not the secret ID");
        }
    }

    private void requireValue(String value, String propertyName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required configuration property: " + propertyName);
        }
    }

    private boolean looksLikeSecretId(String value) {
        return value != null && value.matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    }
}
