package it.pagopa.cruscotto.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.StagingStatus;
import it.pagopa.cruscotto.ingestion.repository.StagingIngestErrorRepository;
import it.pagopa.cruscotto.ingestion.service.ingestion.MissingForeignKeyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
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
                eq(0),
                eq("NAV-1"),
                isNull(),
                isNull()
        );
    }

    /**
     * La coda e' servita in ordine di <strong>ultimo tentativo</strong>, non di creazione: ordinando
     * per {@code CREATED_AT} la prima pagina conteneva sempre gli stessi record piu' vecchi, che se
     * irrecuperabili bloccavano tutto il resto (head-of-line blocking) a qualunque frequenza del job.
     */
    @Test
    void shouldFetchPendingInLeastRecentlyTriedOrder() {
        OffsetDateTime from = OffsetDateTime.now(ZoneOffset.UTC).minusDays(8);
        OffsetDateTime triedBefore = OffsetDateTime.now(ZoneOffset.UTC);

        stagingErrorService.fetchPending(EntityName.POSITION, 50, from, triedBefore);

        ArgumentCaptor<Pageable> page = ArgumentCaptor.forClass(Pageable.class);
        verify(stagingIngestErrorRepository).findPendingLeastRecentlyTried(
                eq(EntityName.POSITION.name()), eq(StagingStatus.PENDING), eq(from), eq(triedBefore),
                page.capture());
        assertThat(page.getValue().getPageSize()).isEqualTo(50);
        assertThat(page.getValue().getPageNumber()).isZero();
    }

    /**
     * Lo sblocco dei PARKED non azzera {@code LAST_RETRY_AT}: e' la chiave dell'ordinamento equo.
     * Azzerandola — come faceva prima — il record tornava in testa alla coda con la sua vecchia
     * {@code CREATED_AT}, e ogni ciclo di unpark riproduceva il blocco che l'ordinamento elimina.
     */
    @Test
    void unparkMustNotResetLastRetryAt() {
        stagingErrorService.unparkOldRecords(Duration.ofMinutes(30), 100,
                OffsetDateTime.now(ZoneOffset.UTC).minusDays(8));

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sql.capture(), any(Object[].class));
        assertThat(sql.getValue()).contains("SET STATUS = ?, RETRY_COUNT = 0");
        assertThat(sql.getValue()).doesNotContain("LAST_RETRY_AT = NULL");
        // Il bound su CREATED_AT e' quello che consente il pruning: COALESCE nasconde al planner la
        // chiave di partizionamento, quindi senza di esso si aprirebbero tutte le 730 partizioni.
        assertThat(sql.getValue()).contains("CREATED_AT >= ?");
    }

    @Test
    void shouldStoreDiscardedRecordsAsDone() {
        RunContext ctx = new RunContext("EXTRA_INFO", "run-2", Instant.now());
        ctx.setOperationId("op-2");

        stagingErrorService.insertDiscardedBulk(
                ctx,
                List.of(new StagingErrorService.DiscardedInputRecord(
                        "source-discarded",
                        Map.of("INFO_NAME", "email"),
                        "EXTRA_INFO not in whitelist"))
        );

        verify(jdbcTemplate).batchUpdate(
                anyString(),
                any(org.springframework.jdbc.core.BatchPreparedStatementSetter.class)
        );
    }
}

