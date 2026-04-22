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
        ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                clusterUrl, appId, appKey, tenantId);
        return ClientFactory.createClient(kcsb);
    }
}
