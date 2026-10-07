package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Copertura del rinfresco delle statistiche sui padri partizionati.
 *
 * <p>Il job esiste perche' autovacuum non analizza mai i padri partizionati, e senza quelle
 * statistiche il planner capovolge i piani: misurato in collaudo, il report dei tentativi passava da
 * 5 minuti (interrotto dal timeout) a 5 millisecondi dopo un solo {@code ANALYZE}. I test qui bloccano
 * le proprieta' che rendono il job utilizzabile in esercizio: un tetto lato server su ogni tabella,
 * indipendenza fra le tabelle, e il rifiuto di nomi che finirebbero in SQL non parametrizzati.</p>
 */
class StatisticsRefreshServiceTest {

    private JdbcTemplate jdbcTemplate;
    private IngestionConfig ingestionConfig;
    private StatisticsRefreshService service;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        DbSchemaConfig schemaConfig = new DbSchemaConfig();
        schemaConfig.setSchema("ingestor");
        ingestionConfig = new IngestionConfig();

        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());

        service = new StatisticsRefreshService(jdbcTemplate, schemaConfig, ingestionConfig, transactionManager);
    }

    @Test
    void analyzesEveryConfiguredParentQualifiedWithTheSchema() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION", "EVENTS_WF"));

        assertThat(service.refresh("run-1")).isEqualTo(2);

        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(4)).execute(statements.capture());
        assertThat(statements.getAllValues())
            .contains("ANALYZE ingestor.POSITION", "ANALYZE ingestor.EVENTS_WF");
    }

    /**
     * Il tetto va applicato <strong>lato server</strong> su ogni tabella: il solo {@code socketTimeout}
     * del client non cancella la query, la lascia orfana. E' la stessa ragione per cui la retention
     * dello staging lo imposta su ogni batch.
     */
    @Test
    void appliesAServerSideStatementTimeoutPerTable() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION"));
        ingestionConfig.getStatisticsRefresh().setStatementTimeout(Duration.ofMinutes(3));

        service.refresh("run-1");

        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(2)).execute(statements.capture());
        // SET LOCAL, non SET: vale per la transazione e non inquina la connessione del pool.
        assertThat(statements.getAllValues()).contains("SET LOCAL statement_timeout = '180000ms'");
    }

    /**
     * Il timeout e' espresso in millisecondi: {@code toSeconds()} troncherebbe a 0 qualunque valore
     * sotto il secondo, e 0 per PostgreSQL significa <em>nessun limite</em> — l'opposto. E' la stessa
     * trappola gia' incontrata su {@code statement-timeout} e sul budget della riconciliazione.
     */
    @Test
    void aSubMillisecondTimeoutDoesNotBecomeNoLimit() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION"));
        ingestionConfig.getStatisticsRefresh().setStatementTimeout(Duration.ofNanos(1));

        service.refresh("run-1");

        ArgumentCaptor<String> statements = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate, times(2)).execute(statements.capture());
        assertThat(statements.getAllValues()).contains("SET LOCAL statement_timeout = '1ms'");
    }

    /**
     * Le statistiche di ciascuna tabella sono indipendenti: averne quattro su cinque aggiornate e'
     * meglio di zero, quindi una tabella che fallisce non deve fermare le altre.
     */
    @Test
    void oneFailingTableDoesNotStopTheOthers() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION", "EVENTS_WF"));
        doThrow(new RuntimeException("statement timeout"))
            .when(jdbcTemplate).execute("ANALYZE ingestor.POSITION");

        assertThat(service.refresh("run-1")).isEqualTo(1);

        verify(jdbcTemplate).execute("ANALYZE ingestor.EVENTS_WF");
    }

    /**
     * Un identificatore non puo' essere un bind parameter, quindi i nomi dalla configurazione finiscono
     * in SQL per concatenazione: il filtro e' obbligatorio, non difensivo.
     */
    @Test
    void rejectsTableNamesThatAreNotPlainIdentifiers() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION; DROP TABLE ingestor.POSITION"));

        assertThat(service.refresh("run-1")).isZero();

        verify(jdbcTemplate, never()).execute(anyString());
    }

    @Test
    void doesNothingWhenDisabled() {
        ingestionConfig.getStatisticsRefresh().setEnabled(false);

        assertThat(service.refresh("run-1")).isZero();
        verify(jdbcTemplate, never()).execute(anyString());
    }

    @Test
    void doesNothingWhenNoTableIsConfigured() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of());

        assertThat(service.refresh("run-1")).isZero();
        verify(jdbcTemplate, never()).execute(anyString());
    }
}
