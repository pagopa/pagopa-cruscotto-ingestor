package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TokenRegistryCleanupServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private DbSchemaConfig dbSchemaConfig;

    @Mock
    private IngestionConfig ingestionConfig;

    private TokenRegistryCleanupService service() {
        return new TokenRegistryCleanupService(jdbcTemplate, dbSchemaConfig, ingestionConfig);
    }

    private IngestionConfig.TokenRegistryCleanupConfig config(boolean enabled, Duration retention) {
        IngestionConfig.TokenRegistryCleanupConfig cfg = new IngestionConfig.TokenRegistryCleanupConfig();
        cfg.setEnabled(enabled);
        cfg.setRetention(retention);
        return cfg;
    }

    @Test
    void computesCutoffFromDurationRetentionWithoutThrowing() {
        // Regression: retention is a Duration; the old LocalDate.minus(Duration) threw
        // UnsupportedTemporalTypeException ("Unsupported unit: Seconds") and the purge never ran.
        lenient().when(dbSchemaConfig.getSchemaName()).thenReturn("sert_ingestor");
        when(ingestionConfig.getTokenRegistryCleanup()).thenReturn(config(true, Duration.ofDays(7)));
        when(jdbcTemplate.update(anyString(), org.mockito.ArgumentMatchers.<Object>any())).thenReturn(42);

        LocalDate expectedCutoff = LocalDate.now(ZoneOffset.UTC).minusDays(7);
        int deleted = service().cleanup("run-purge");

        assertEquals(42, deleted);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> argCaptor = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), argCaptor.capture());
        assertEquals("DELETE FROM sert_ingestor.POSITION_TOKEN_REGISTRY WHERE FIRST_DATE_EVENT < ?",
                sqlCaptor.getValue());
        assertEquals(expectedCutoff, argCaptor.getValue());
    }

    @Test
    void skipsWhenDisabled() {
        when(ingestionConfig.getTokenRegistryCleanup()).thenReturn(config(false, Duration.ofDays(7)));

        int deleted = service().cleanup("run-purge");

        assertEquals(0, deleted);
        verify(jdbcTemplate, never()).update(anyString(), org.mockito.ArgumentMatchers.<Object>any());
    }

    @Test
    void skipsWhenConfigMissing() {
        when(ingestionConfig.getTokenRegistryCleanup()).thenReturn(null);

        int deleted = service().cleanup("run-purge");

        assertEquals(0, deleted);
        verifyNoInteractions(jdbcTemplate);
    }
}
