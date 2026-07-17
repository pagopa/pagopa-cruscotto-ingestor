package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExecutionLogServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private DbSchemaConfig dbSchemaConfig;

    private ExecutionLogService executionLogService;

    @BeforeEach
    void setUp() {
        when(dbSchemaConfig.getSchemaName()).thenReturn("ingestor");
        executionLogService = new ExecutionLogService(jdbcTemplate, dbSchemaConfig);
        ReflectionTestUtils.setField(executionLogService, "enabled", true);
    }

    @Test
    void logFailedShouldBindAllUpdateParameters() {
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);
        RunContext ctx = new RunContext("POSITION_TOKENS", "run-1", Instant.now());
        ctx.incrementAnagraficaLookupCount();
        ctx.incrementPositionLookupCount();
        ctx.incrementTokenLookupCount();
        ctx.incrementCacheHitCount();
        ctx.incrementCacheMissCount();
        ctx.incrementAdxWindowCount();
        ctx.addAdxAttemptCount(2);
        ctx.incrementEmptyWindowCount();

        executionLogService.logFailed(
                ctx,
                "ERR",
                "boom",
                10,
                8,
                3,
                2,
                1,
                5,
                4
        );

        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate, times(1)).update(anyString(), argsCaptor.capture());
        assertEquals(30, argsCaptor.getValue().length);
    }
}
