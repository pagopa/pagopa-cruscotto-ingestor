package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BatchMetadataCleanupServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private PlatformTransactionManager transactionManager;

    private IngestionConfig ingestionConfig;
    private BatchMetadataCleanupService service;

    @BeforeEach
    void setUp() {
        DbSchemaConfig dbSchemaConfig = new DbSchemaConfig();
        dbSchemaConfig.setSchema("ingestor");

        ingestionConfig = new IngestionConfig();
        IngestionConfig.BatchMetadataCleanupConfig config = new IngestionConfig.BatchMetadataCleanupConfig();
        config.setEnabled(true);
        config.setBatchSize(2);
        config.setRetention(Duration.ofDays(7));
        ingestionConfig.setBatchMetadataCleanup(config);

        service = new BatchMetadataCleanupService(jdbcTemplate, dbSchemaConfig, ingestionConfig, transactionManager);
    }

    @Test
    void shouldNoopWhenDisabled() {
        ingestionConfig.getBatchMetadataCleanup().setEnabled(false);

        int deleted = service.cleanup("run-1");

        assertEquals(0, deleted);
        verify(jdbcTemplate, never()).queryForList(anyString(), eq(Long.class), any(), any());
    }

    @Test
    void shouldDeleteInBatchesUntilLastPartialBatch() {
        when(jdbcTemplate.queryForList(anyString(), eq(Long.class), any(), any()))
                .thenReturn(List.of(1L, 2L))
                .thenReturn(List.of(3L, 4L))
                .thenReturn(List.of(5L));
        when(transactionManager.getTransaction(any())).thenReturn(null);

        int deleted = service.cleanup("run-2");

        assertEquals(5, deleted);
        verify(jdbcTemplate, times(3)).queryForList(anyString(), eq(Long.class), any(), any());
        verify(jdbcTemplate, times(1)).update(contains("JOB_INSTANCE"));
    }

    @Test
    void shouldStopImmediatelyWhenNoRows() {
        when(jdbcTemplate.queryForList(anyString(), eq(Long.class), any(), any()))
                .thenReturn(List.of());
        when(transactionManager.getTransaction(any())).thenReturn(null);

        int deleted = service.cleanup("run-3");

        assertEquals(0, deleted);
        verify(jdbcTemplate, times(1)).queryForList(anyString(), eq(Long.class), any(), any());
        verify(jdbcTemplate, never()).update(contains("STEP_EXECUTION"), any(Object[].class));
    }

    @Test
    void shouldDeleteChildTablesBeforeParentInFkOrder() {
        when(jdbcTemplate.queryForList(anyString(), eq(Long.class), any(), any()))
                .thenReturn(List.of(1L))
                .thenReturn(List.of());
        when(transactionManager.getTransaction(any())).thenReturn(null);

        service.cleanup("run-4");

        InOrder ordered = inOrder(jdbcTemplate);
        ordered.verify(jdbcTemplate).update(contains("STEP_EXECUTION_CONTEXT"), any(Object[].class));
        ordered.verify(jdbcTemplate).update(contains("BATCH_STEP_EXECUTION WHERE"), any(Object[].class));
        ordered.verify(jdbcTemplate).update(contains("JOB_EXECUTION_CONTEXT"), any(Object[].class));
        ordered.verify(jdbcTemplate).update(contains("JOB_EXECUTION_PARAMS"), any(Object[].class));
        ordered.verify(jdbcTemplate).update(contains("BATCH_JOB_EXECUTION WHERE"), any(Object[].class));
    }
}
