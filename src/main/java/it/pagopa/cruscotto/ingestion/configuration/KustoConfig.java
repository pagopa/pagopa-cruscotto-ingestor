package it.pagopa.cruscotto.ingestion.configuration;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.HttpClientProperties;
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
        return ClientFactory.createClient(kcsb, httpClientProperties());
    }

    /**
     * Explicit HTTP connection-pool tuning for the (singleton) Kusto client. Without this the SDK
     * falls back to Apache HttpClient defaults, which (a) leave idle connections in the pool until
     * the Azure load balancer silently drops them — producing intermittent "Connection reset"
     * errors, especially with the sparse prod schedule (every 30m/1h) — and (b) cap concurrent
     * connections per route low enough to serialize the parallel Quartz workers.
     *
     * <ul>
     *   <li>{@code maxIdleTime}/{@code keepAlive} (seconds): evict connections idle beyond 60s,
     *       well under typical Azure LB idle timeouts, so a stale socket is never reused.</li>
     *   <li>{@code maxConnectionsTotal}/{@code PerRoute}: headroom above the Quartz thread pool so
     *       concurrent entity imports do not queue on the connection pool.</li>
     * </ul>
     */
    private HttpClientProperties httpClientProperties() {
        return HttpClientProperties.builder()
                .maxConnectionsTotal(50)
                .maxConnectionsPerRoute(50)
                .keepAlive(true)
                .maxKeepAliveTime(120)
                .maxIdleTime(60)
                .build();
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
