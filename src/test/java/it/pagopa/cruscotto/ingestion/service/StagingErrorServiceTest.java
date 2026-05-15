package it.pagopa.cruscotto.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.repository.StagingIngestErrorRepository;
import it.pagopa.cruscotto.ingestion.service.ingestion.MissingForeignKeyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class StagingErrorServiceTest {

    @Mock
    private StagingIngestErrorRepository stagingIngestErrorRepository;

    @Mock
    private JdbcTemplate jdbcTemplate;

    private StagingErrorService stagingErrorService;

    @BeforeEach
    void setUp() {
        DbSchemaConfig dbSchemaConfig = new DbSchemaConfig();
        dbSchemaConfig.setSchema("ingestor");
        stagingErrorService = new StagingErrorService(stagingIngestErrorRepository, new ObjectMapper(), jdbcTemplate, dbSchemaConfig);
    }

    @Test
    void shouldStoreMissingForeignKeyErrorCode() {
        RunContext ctx = new RunContext("EVENTS_WF", "run-1", Instant.now());
        ctx.setOperationId("op-1");

        stagingErrorService.insertError(
                ctx,
                "source-1",
                Map.of("NAV", "NAV-1"),
                new MissingForeignKeyException("Missing required FK fkPosition")
        );

        verify(jdbcTemplate).update(
                anyString(),
                eq("run-1"),
                eq("EVENTS_WF"),
                eq("source-1"),
                eq("op-1"),
                anyString(),
                eq("MISSING_FOREIGN_KEY"),
                eq("Missing required FK fkPosition"),
                any(),
                eq("PENDING"),
                eq(0)
        );
    }
}


