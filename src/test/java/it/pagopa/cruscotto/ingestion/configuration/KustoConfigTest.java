package it.pagopa.cruscotto.ingestion.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertThrows;

class KustoConfigTest {

    @Test
    void kustoClientShouldRejectSecretIdAsAppKey() {
        KustoConfig config = new KustoConfig();
        ReflectionTestUtils.setField(config, "clusterUrl", "https://example.kusto.windows.net/");
        ReflectionTestUtils.setField(config, "appId", "app-id");
        ReflectionTestUtils.setField(config, "appKey", "8677a3ca-8d05-453f-947c-0bdb1d15e512");
        ReflectionTestUtils.setField(config, "tenantId", "tenant-id");

        IllegalStateException exception = assertThrows(IllegalStateException.class, config::kustoClient);

        org.junit.jupiter.api.Assertions.assertTrue(exception.getMessage().contains("secret value"));
    }
}
