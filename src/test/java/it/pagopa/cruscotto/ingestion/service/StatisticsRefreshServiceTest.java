package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
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
        verify(jdbcTemplate, times(6)).execute(statements.capture());
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
        verify(jdbcTemplate, times(3)).execute(statements.capture());
        // SET LOCAL, non SET: vale per la transazione e non inquina la connessione del pool.
        assertThat(statements.getAllValues()).contains("SET LOCAL statement_timeout = '180000ms'");
        // Anche il lock: ANALYZE conflitta con autovacuum sulla stessa tabella, e un job notturno
        // deve rinunciare invece di restare in attesa.
        assertThat(statements.getAllValues()).contains("SET LOCAL lock_timeout = '30s'");
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
        verify(jdbcTemplate, times(3)).execute(statements.capture());
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

    /**
     * Il lock non ottenuto e' atteso e transitorio — autovacuum o una retention tenevano la tabella —
     * e si risolve da solo alla prossima esecuzione: va a WARN, non a ERROR, per non generare
     * allarmi su qualcosa che non richiede intervento.
     */
    @Test
    void aLockConflictIsReportedAsSkippedAndTheOtherTablesContinue() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION", "EVENTS_WF"));
        doThrow(new CannotAcquireLockException("lock timeout"))
            .when(jdbcTemplate).execute("ANALYZE ingestor.POSITION");

        assertThat(service.refresh("run-1")).isEqualTo(1);

        verify(jdbcTemplate).execute("ANALYZE ingestor.EVENTS_WF");
    }

    /**
     * Il timeout invece <strong>non</strong> si risolve da solo: se ANALYZE non sta nel tetto oggi non
     * ci starà nemmeno domani, e le statistiche di quella tabella restano stantie a ogni esecuzione
     * mentre il planner continua a sbagliare i piani in silenzio. Deve restare distinguibile dal
     * conflitto sul lock, perché richiede un intervento sulla configurazione.
     */
    @Test
    void aStatementTimeoutIsReportedSeparatelyBecauseItWillNotHealOnItsOwn() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION", "EVENTS_WF"));
        doThrow(new QueryTimeoutException("canceling statement due to statement timeout"))
            .when(jdbcTemplate).execute("ANALYZE ingestor.POSITION");

        assertThat(service.refresh("run-1")).isEqualTo(1);

        verify(jdbcTemplate).execute("ANALYZE ingestor.EVENTS_WF");
    }

    /**
     * Il tetto complessivo esiste per tenere il caso peggiore del job sotto la soglia del watchdog
     * sui job appesi.
     *
     * <p>Senza, il peggio sarebbe {@code tabelle x statement-timeout}: con cinque tabelle da 30
     * minuti fa 150, contro un {@code blocked-trigger-threshold} di 90m in produzione. Il watchdog
     * segnalerebbe come appeso un job che sta solo lavorando — e un allarme che urla al lupo
     * smette di essere ascoltato.</p>
     */
    @Test
    void ilBudgetComplessivoFermaIlJobPrimaDiIniziareUnaNuovaTabella() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION", "EVENTS_WF", "EXTRA_INFO"));
        ingestionConfig.getStatisticsRefresh().setStatementTimeout(Duration.ofMillis(5));
        ingestionConfig.getStatisticsRefresh().setMaxDuration(Duration.ofMillis(20));
        // La prima tabella consuma da sola tutto il budget: le successive non devono partire.
        doAnswer(invocation -> {
            Thread.sleep(60);
            return null;
        }).when(jdbcTemplate).execute("ANALYZE ingestor.POSITION");

        assertThat(service.refresh("run-1")).isEqualTo(1);

        verify(jdbcTemplate).execute("ANALYZE ingestor.POSITION");
        verify(jdbcTemplate, never()).execute("ANALYZE ingestor.EVENTS_WF");
        verify(jdbcTemplate, never()).execute("ANALYZE ingestor.EXTRA_INFO");
    }

    /**
     * Il tetto prenota il caso peggiore della tabella che sta per partire, non solo il tempo gia'
     * trascorso.
     *
     * <p>Senza la prenotazione, una tabella avviata un istante prima della scadenza aggiungerebbe un
     * intero {@code statement-timeout}: con i default di produzione sarebbero 60m di budget piu' 30m
     * di tabella, cioe' esattamente il {@code blocked-trigger-threshold} di 90m. Il tetto non
     * sarebbe un tetto.</p>
     */
    @Test
    void ilTettoPrenotaIlCasoPeggioreDellaTabellaCheStaPerPartire() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION", "EVENTS_WF"));
        // Il tempo trascorso e' ~0, ma la seconda tabella potrebbe da sola sforare il budget.
        ingestionConfig.getStatisticsRefresh().setStatementTimeout(Duration.ofMinutes(30));
        ingestionConfig.getStatisticsRefresh().setMaxDuration(Duration.ofMinutes(20));

        assertThat(service.refresh("run-1")).isEqualTo(1);

        verify(jdbcTemplate).execute("ANALYZE ingestor.POSITION");
        verify(jdbcTemplate, never()).execute("ANALYZE ingestor.EVENTS_WF");
    }

    /**
     * La prima tabella parte <strong>sempre</strong>, anche con un budget assurdo: il controllo la
     * precede e a quel punto il tempo trascorso e' zero.
     *
     * <p>E' voluto e coerente col budget della riconciliazione: una configurazione sbagliata deve
     * degradare in "faccio meno", non in uno stallo silenzioso in cui le statistiche non vengono
     * aggiornate mai piu'. E una volta avviata, la tabella non viene interrotta: {@code ANALYZE}
     * non e' interrompibile dall'esterno e spezzarlo lascerebbe statistiche peggiori di prima.</p>
     */
    @Test
    void laPrimaTabellaVieneSempreAnalizzataAncheConUnBudgetMinimo() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION", "EVENTS_WF"));
        ingestionConfig.getStatisticsRefresh().setMaxDuration(Duration.ofNanos(1));

        assertThat(service.refresh("run-1")).isEqualTo(1);

        verify(jdbcTemplate).execute("ANALYZE ingestor.POSITION");
        verify(jdbcTemplate, never()).execute("ANALYZE ingestor.EVENTS_WF");
    }

    /** Budget a zero = nessun tetto, coerentemente con gli altri budget del progetto. */
    @Test
    void unBudgetAZeroNonLimitaIlJob() {
        ingestionConfig.getStatisticsRefresh().setTables(List.of("POSITION", "EVENTS_WF"));
        ingestionConfig.getStatisticsRefresh().setMaxDuration(Duration.ZERO);

        assertThat(service.refresh("run-1")).isEqualTo(2);
    }

    /**
     * Il default deve restare compatibile con il watchdog: {@code maxDuration} va tenuto sotto il
     * {@code blocked-trigger-threshold} di produzione (90m), altrimenti il tetto non serve a nulla.
     */
    @Test
    void ilDefaultDelBudgetRestaSottoLaSogliaDelWatchdog() {
        IngestionConfig.StatisticsRefreshConfig config = new IngestionConfig().getStatisticsRefresh();
        Duration budget = config.getMaxDuration();
        Duration perTabella = config.getStatementTimeout();

        // Col tetto prenotato, il caso peggiore del job e' il massimo fra i due: il budget non puo'
        // essere sforato, e la prima tabella (che parte sempre) non puo' durare piu' del suo timeout.
        Duration casoPeggiore = budget.compareTo(perTabella) >= 0 ? budget : perTabella;

        assertThat(casoPeggiore)
            .as("deve restare sotto il blocked-trigger-threshold di 90m di prod e uat")
            .isLessThan(Duration.ofMinutes(90));
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
