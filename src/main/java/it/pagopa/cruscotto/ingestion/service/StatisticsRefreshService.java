package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Aggiorna le statistiche del planner sui <strong>padri partizionati</strong>.
 *
 * <p><strong>Perche' serve un job dedicato.</strong> Autovacuum analizza le singole partizioni, ma
 * <em>non raccoglie mai</em> le statistiche del padre partizionato — ed e' il padre che il planner usa
 * per pianificare qualunque query sulla tabella logica. Su tabelle che crescono di milioni di righe al
 * giorno quelle statistiche sono stantie per costruzione, e nessun meccanismo automatico le aggiorna.
 *
 * <p><strong>Cosa costava non farlo.</strong> Misurato in collaudo sul report dei tentativi della
 * ricerca massiva: con le statistiche assenti il planner stimava 1.094.664 righe dal join sulle chiavi
 * invece di 2, concludeva che restringere per chiave non servisse, e partiva da una scansione di tutti
 * i 7,3 milioni di token espandendoli attraverso tre LATERAL — <strong>5 minuti, interrotto dal
 * timeout</strong>. Dopo un solo {@code ANALYZE}, stessa query non modificata: costo stimato da 1e14 a
 * 1.686, <strong>5 millisecondi</strong>. Le statistiche non sono un'ottimizzazione marginale su queste
 * tabelle, sono la differenza fra un piano sensato e uno inservibile.
 *
 * <p><strong>Perche' e' sicuro durante l'ingestion.</strong> {@code ANALYZE} prende un lock
 * {@code SHARE UPDATE EXCLUSIVE}, compatibile con il {@code ROW EXCLUSIVE} di INSERT e UPDATE: non
 * blocca ne' letture ne' scritture. Conflitta solo con altra manutenzione sulla stessa tabella
 * (VACUUM, altro ANALYZE, CREATE INDEX, ALTER TABLE), da cui lo slot orario separato dalle retention.
 * E non scansiona: campiona un numero di righe proporzionale a {@code default_statistics_target}.
 */
@Slf4j
@Service
public class StatisticsRefreshService {

    /**
     * I nomi arrivano dalla configurazione e finiscono in SQL <strong>non parametrizzati</strong>:
     * un identificatore non puo' essere un bind parameter. Il filtro e' quindi obbligatorio, non
     * difensivo.
     */
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    /** Non vale la pena attendere dietro un autovacuum: si rinuncia e si riprova la notte dopo. */
    private static final String LOCK_TIMEOUT = "30s";

    /** Il valore di {@code event_action} per questo job. */
    private static final String ENTITY_NAME = "STATISTICS_REFRESH";

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;
    private final IngestionConfig ingestionConfig;
    private final TransactionTemplate transactionTemplate;

    public StatisticsRefreshService(JdbcTemplate jdbcTemplate,
                                    DbSchemaConfig dbSchemaConfig,
                                    IngestionConfig ingestionConfig,
                                    PlatformTransactionManager transactionManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbSchemaConfig = dbSchemaConfig;
        this.ingestionConfig = ingestionConfig;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /** @return numero di tabelle analizzate con successo */
    public int refresh(String runId) {
        // Un solo scope per tutta l'esecuzione: correlation_id ed event_action valgono per ogni
        // riga emessa qui dentro, e ripeterli a ogni chiamata sarebbe la duplicazione che questo
        // lavoro ha eliminato altrove.
        try (MdcScope ignored = MdcScope.open()
                .with(LogFields.CORRELATION_ID, runId)
                .with(LogFields.EVENT_ACTION, ENTITY_NAME)) {
            return refreshInScope(runId);
        }
    }

    private int refreshInScope(String runId) {
        IngestionConfig.StatisticsRefreshConfig config = ingestionConfig.getStatisticsRefresh();
        if (!config.isEnabled()) {
            try (MdcScope ignored = phase("NOOP")) {
                log.info("Skipped statistics refresh, disabled by configuration");
            }
            return 0;
        }

        List<String> tables = config.getTables();
        if (tables == null || tables.isEmpty()) {
            try (MdcScope ignored = phase("NOOP")) {
                log.warn("Skipped statistics refresh, no table configured");
            }
            return 0;
        }

        String timeout = timeoutLiteral(config.getStatementTimeout());
        long startedNanos = System.nanoTime();
        int analyzed = 0;
        int failed = 0;

        long budgetMs = budgetMillis(config.getMaxDuration());
        for (int i = 0; i < tables.size(); i++) {
            String table = tables.get(i);
            // Il budget si verifica PRIMA di iniziare una tabella, mai durante: ANALYZE non e'
            // interrompibile dall'esterno e spezzarlo a meta' lascerebbe statistiche peggiori di
            // quelle di partenza.
            //
            // Si prenota il caso peggiore della tabella che sta per partire, non solo il tempo gia'
            // trascorso: altrimenti una tabella avviata un istante prima della scadenza potrebbe
            // aggiungere un intero statement-timeout e portare il totale a budget + timeout, cioe'
            // 60m + 30m = esattamente la soglia del watchdog. Cosi' invece maxDuration e' un tetto
            // vero.
            //
            // La PRIMA tabella parte comunque: una configurazione sbagliata deve degradare in
            // "faccio meno", non in statistiche che non si aggiornano mai piu'. E' lo stesso
            // principio del budget della riconciliazione, che tenta sempre almeno un record.
            long reservedMs = elapsedMs(startedNanos) + Math.max(0L, config.getStatementTimeout() == null
                    ? 0L : config.getStatementTimeout().toMillis());
            if (i > 0 && budgetMs > 0 && reservedMs > budgetMs) {
                try (MdcScope ignored = phase("BUDGET_EXCEEDED");
                     LogDetails details = LogDetails.open()
                             .with("max_duration", config.getMaxDuration())
                             .with(LogFields.ELAPSED_MS, elapsedMs(startedNanos))
                             .with("analyzed", analyzed)
                             .with("skipped", String.valueOf(tables.subList(i, tables.size())))) {
                    log.warn("Stopped statistics refresh on the duration budget, "
                            + "the remaining tables keep the previous run statistics");
                }
                break;
            }
            if (!SAFE_IDENTIFIER.matcher(table).matches()) {
                // Non e' un errore recuperabile: e' una configurazione sbagliata, e va vista.
                try (MdcScope ignored = phase("ERROR");
                     LogDetails details = LogDetails.open().with(LogFields.TABLE, table)) {
                    log.error("Ignored table, the configured name is not a valid identifier");
                }
                failed++;
                continue;
            }
            String qualified = dbSchemaConfig.getSchemaName() + "." + table;
            long tableStartedNanos = System.nanoTime();
            try {
                // ANALYZE, a differenza di VACUUM, e' ammesso dentro una transazione: la transazione
                // serve a dare un senso al SET LOCAL, che altrimenti sporcherebbe una connessione del
                // pool (o finirebbe su una connessione diversa da quella dello statement).
                transactionTemplate.executeWithoutResult(status -> {
                    jdbcTemplate.execute("SET LOCAL statement_timeout = '" + timeout + "'");
                    // ANALYZE chiede SHARE UPDATE EXCLUSIVE, che conflitta con autovacuum sulla stessa
                    // tabella: senza questo tetto resterebbe in attesa dietro un VACUUM che su una
                    // partizione grande dura minuti. Per un job notturno e' meglio rinunciare e
                    // riprovare domani che occupare lo slot, e l'errore dice "lock non ottenuto"
                    // invece di un timeout generico.
                    jdbcTemplate.execute("SET LOCAL lock_timeout = '" + LOCK_TIMEOUT + "'");
                    jdbcTemplate.execute("ANALYZE " + qualified);
                });
                analyzed++;
                try (MdcScope ignored = phase("ANALYZED");
                     LogDetails details = tableDetails(qualified, tableStartedNanos)) {
                    log.info("Analyzed partitioned parent table");
                }
            } catch (CannotAcquireLockException exception) {
                // Atteso e transitorio: autovacuum o una retention tenevano la tabella. Si riprende
                // alla prossima esecuzione, quindi non e' un errore da svegliare qualcuno.
                failed++;
                try (MdcScope ignored = phase("SKIPPED");
                     LogDetails details = tableDetails(qualified, tableStartedNanos)
                             .with("lock_timeout", LOCK_TIMEOUT)) {
                    log.warn("Skipped table, lock not acquired (autovacuum or retention on the same "
                            + "table): it is retried on the next run");
                }
            } catch (QueryTimeoutException exception) {
                // NON si risolve da solo: se ANALYZE non sta nel tetto, non ci stara' nemmeno domani.
                // Le statistiche di questa tabella restano stantie e il planner continua a sbagliare i
                // piani, in silenzio. Il log deve dire la leva, altrimenti il problema si vede solo
                // come query lente settimane dopo.
                failed++;
                try (MdcScope ignored = phase("TIMEOUT");
                     LogDetails details = tableDetails(qualified, tableStartedNanos)
                             .with("statement_timeout", timeout)) {
                    log.error("ANALYZE not completed within the statement timeout: the statistics of "
                            + "this table stay STALE and the problem recurs on every run. Raise "
                            + "ingestion.statistics-refresh.statement-timeout "
                            + "(INGESTION_STATISTICS_REFRESH_STATEMENT_TIMEOUT)", exception);
                }
            } catch (Exception exception) {
                // Una tabella che fallisce non deve impedire le altre: le statistiche di ciascuna sono
                // indipendenti, e averne quattro su cinque aggiornate e' meglio di zero.
                failed++;
                try (MdcScope ignored = phase("ERROR");
                     LogDetails details = tableDetails(qualified, tableStartedNanos)
                             .with(LogFields.ERROR_DETAIL, exception.getMessage())) {
                    log.error("Failed to analyze table", exception);
                }
            }
        }

        try (MdcScope ignored = phase("END");
             LogDetails details = LogDetails.open()
                     .with("analyzed", analyzed)
                     .with("failed", failed)
                     .with(LogFields.ELAPSED_MS, elapsedMs(startedNanos))) {
            log.info("Completed statistics refresh");
        }
        return analyzed;
    }

    /** La fase corrente, come attributo di primo livello invece che dentro il testo. */
    private static MdcScope phase(String phase) {
        return MdcScope.open().with(LogFields.CTX_PHASE, phase);
    }

    private static LogDetails tableDetails(String qualifiedTable, long tableStartedNanos) {
        return LogDetails.open()
                .with(LogFields.DEPENDENCY, "postgres")
                .with(LogFields.TABLE, qualifiedTable)
                .with(LogFields.ELAPSED_MS, elapsedMs(tableStartedNanos));
    }

    /**
     * Il timeout e' espresso in <strong>millisecondi</strong>: {@code toSeconds()} troncherebbe a 0
     * — che per PostgreSQL significa <em>nessun limite</em> — qualunque valore sotto il secondo.
     */
    private static String timeoutLiteral(Duration statementTimeout) {
        if (statementTimeout == null || statementTimeout.isZero() || statementTimeout.isNegative()) {
            return "0";
        }
        return Math.max(1L, statementTimeout.toMillis()) + "ms";
    }

    /**
     * Millisecondi di budget complessivo; 0 disattiva il tetto.
     *
     * <p>Il clamp a 1 e volutamente lo stesso idioma usato altrove: { toMillis()} di una
     * durata sotto il millisecondo darebbe 0, che nel predicato significa <em>nessun limite</em> —
     * l'opposto di quanto configurato.</p>
     */
    private static long budgetMillis(java.time.Duration maxDuration) {
        if (maxDuration == null || maxDuration.isZero() || maxDuration.isNegative()) {
            return 0L;
        }
        return Math.max(1L, maxDuration.toMillis());
    }

    private static long elapsedMs(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
