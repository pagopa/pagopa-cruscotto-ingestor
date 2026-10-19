package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Applica la retention a {@code STG_INGEST_ERROR}, in batch limitati e ognuno nella propria
 * transazione.
 *
 * <p><strong>Perche' a batch.</strong> La forma precedente era una singola {@code DELETE} su tutta la
 * tabella in un'unica transazione. In produzione quella query ha superato il {@code socketTimeout} del
 * driver (360 s) <em>ogni notte</em>: il client mollava, PostgreSQL non riceveva alcun ordine di
 * cancellazione, la transazione veniva abortita e la connessione distrutta. L'errore osservato era
 * {@code Unable to rollback against JDBC Connection ... Connection is closed} con una durata di
 * esattamente <strong>720 s = 2 x socketTimeout</strong> (uno per la DELETE, uno per il rollback
 * tentato sulla connessione gia' chiusa). Risultato: zero righe cancellate per giorni, con la tabella
 * che accumulava indefinitamente.</p>
 *
 * <p>Con i batch ogni commit e' progresso acquisito: un fallimento a metà non annulla il lavoro gia'
 * fatto e la notte successiva riprende da dove si era fermata.</p>
 *
 * <p><strong>Perche' i timeout lato server.</strong> Su questo percorso non era applicato alcun
 * {@code statement_timeout} ({@code ingestion.persistence.statement-timeout} vale solo per il path di
 * scrittura dell'ingestion), quindi l'unico limite era quello del client — che <em>non cancella</em> la
 * query e la lascia orfana sul server. Con {@code SET LOCAL} e' il server a interrompere, con un errore
 * pulito, mantenendo la connessione riutilizzabile. Il {@code lock_timeout} serve perche' la
 * riconciliazione aggiorna le stesse righe ogni ora: senza di esso la DELETE puo' attendere
 * indefinitamente dietro i suoi lock.</p>
 */
@Slf4j
@Service
public class StagingErrorCleanupService {

    /**
     * Tetto per singolo batch, tenuto sotto il {@code socketTimeout} del client (360 s) cosi' e'
     * sempre il server a interrompere per primo, con un errore diagnosticabile.
     */
    private static final String BATCH_STATEMENT_TIMEOUT = "60s";

    /** Un batch non deve restare appeso dietro i lock della riconciliazione: meglio ritentarlo. */
    private static final String BATCH_LOCK_TIMEOUT = "10s";

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;
    private final IngestionConfig ingestionConfig;
    private final TransactionTemplate transactionTemplate;

    public StagingErrorCleanupService(JdbcTemplate jdbcTemplate,
                                      DbSchemaConfig dbSchemaConfig,
                                      IngestionConfig ingestionConfig,
                                      PlatformTransactionManager transactionManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbSchemaConfig = dbSchemaConfig;
        this.ingestionConfig = ingestionConfig;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /** @return numero di righe effettivamente rimosse */
    public long cleanup(String runId) {
        IngestionConfig.StagingErrorCleanupConfig config = ingestionConfig.getStagingErrorCleanup();
        if (config == null || !config.isEnabled()) {
            try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "NOOP")) {
                log.info("Skipped cleanup, disabled by configuration");
            }
            return 0;
        }

        OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minus(config.getRetention());
        // La tabella e' partizionata per giorno dalla migration 47, ma il codice non assume che la
        // migration sia gia' passata: se non lo e' (deploy del codice prima della migration, o
        // ambiente locale non aggiornato) si ricade sulla DELETE a batch, che resta corretta.
        if (isPartitioned()) {
            return truncateExpiredPartitions(runId, threshold);
        }
        try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "FALLBACK")) {
            log.info("Applied retention with batched DELETE, the table is not partitioned");
        }
        int limit = Math.max(1, config.getBatchSize());
        Duration maxDuration = config.getMaxDuration();
        String table = dbSchemaConfig.getSchemaName() + ".STG_INGEST_ERROR";
        // ORDER BY CREATED_AT (non ID) per percorrere l'indice su CREATED_AT: con ORDER BY ID il
        // planner dovrebbe ordinare l'intero insieme selezionato a ogni batch.
        String sql = "DELETE FROM " + table + " WHERE ID IN "
                + "(SELECT ID FROM " + table + " WHERE CREATED_AT < ? ORDER BY CREATED_AT LIMIT ?)";

        long startedNanos = System.nanoTime();
        long totalDeleted = 0;
        int deleted;
        boolean exhausted;
        do {
            deleted = deleteBatch(sql, threshold, limit);
            totalDeleted += deleted;
            exhausted = deleted < limit;
        } while (!exhausted && !isOverBudget(runId, startedNanos, maxDuration, totalDeleted));

        try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "END");
             LogDetails details = LogDetails.open()
                     .with(LogFields.DEPENDENCY, "postgres")
                     .with("retention", config.getRetention())
                     .with("threshold", threshold)
                     .with("batch_size", limit)
                     .with("deleted", totalDeleted)
                     .with(LogFields.ELAPSED_MS, elapsedMs(startedNanos))
                     .with("exhausted", exhausted)) {
            log.info("Completed staging error cleanup with batched DELETE");
        }
        return totalDeleted;
    }

    /** @return {@code true} se {@code STG_INGEST_ERROR} e' una tabella partizionata ({@code relkind='p'}) */
    private boolean isPartitioned() {
        Boolean partitioned = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                        + " WHERE n.nspname = ? AND c.relname = 'stg_ingest_error' AND c.relkind = 'p')",
                Boolean.class, dbSchemaConfig.getSchemaName());
        return Boolean.TRUE.equals(partitioned);
    }

    /**
     * Svuota con {@code TRUNCATE} le partizioni giornaliere interamente oltre la retention.
     *
     * <p>Una partizione viene toccata solo se il suo estremo superiore e' {@code <= threshold}: cosi'
     * non si perde mai una riga ancora dentro la finestra. La partizione {@code DEFAULT} non viene mai
     * svuotata (non ha un intervallo di date), ma viene <strong>segnalata</strong> se contiene righe:
     * significa che la copertura delle partizioni pre-create e' esaurita.</p>
     *
     * <p>Prima di svuotare si contano i record {@code PENDING}: sono dati ADX mai riconciliati, che
     * da qui in poi sono persi. Senza questo conteggio la perdita sarebbe silenziosa — e con il
     * {@code TRUNCATE} spariscono un giorno alla volta, istantaneamente.</p>
     */
    private long truncateExpiredPartitions(String runId, OffsetDateTime threshold) {
        List<String> expired = jdbcTemplate.queryForList(
                // pg_get_expr espone i bound della partizione: 'FOR VALUES FROM (..) TO (..)'. Si
                // ricava l'estremo superiore e si confronta con la soglia, invece di dedurre la data
                // dal nome della partizione, che sarebbe una convenzione e non un fatto.
                "SELECT c.relname FROM pg_inherits i"
                        + " JOIN pg_class c ON c.oid = i.inhrelid"
                        + " JOIN pg_class p ON p.oid = i.inhparent"
                        + " JOIN pg_namespace n ON n.oid = p.relnamespace"
                        + " WHERE n.nspname = ? AND p.relname = 'stg_ingest_error'"
                        + "   AND pg_get_expr(c.relpartbound, c.oid) NOT LIKE '%DEFAULT%'"
                        + "   AND (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'TO \\(''([^'']+)''\\)'))[1]::timestamptz <= ?"
                        + " ORDER BY c.relname",
                String.class, dbSchemaConfig.getSchemaName(), threshold);

        long removed = 0;
        for (String partition : expired) {
            removed += truncatePartition(runId, partition);
        }
        warnIfDefaultPartitionHasRows(runId);
        try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "END");
             LogDetails details = LogDetails.open()
                     .with(LogFields.DEPENDENCY, "postgres")
                     .with("threshold", threshold)
                     .with("partitions", expired.size())
                     .with("removed", removed)) {
            log.info("Completed staging error cleanup by truncating partitions");
        }
        return removed;
    }

    /** Svuota una partizione, registrando quanti record non riconciliati vanno perduti. */
    private long truncatePartition(String runId, String partition) {
        String qualified = dbSchemaConfig.getSchemaName() + "." + quoteIdentifier(partition);
        Long pending = transactionTemplate.execute(status -> jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + qualified + " WHERE STATUS <> 'DONE'", Long.class));
        Long total = transactionTemplate.execute(status -> jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + qualified, Long.class));
        long rows = total != null ? total : 0;
        if (rows == 0) {
            return 0;
        }
        if (pending != null && pending > 0) {
            try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "DATA_LOSS");
                 LogDetails details = LogDetails.open()
                         .with("partition", partition)
                         .with("pending_records", pending)
                         .with(LogFields.ROWS, rows)) {
                log.warn("Emptying a partition with records never reconciled: they exceeded retention");
            }
        }
        transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.execute("SET LOCAL lock_timeout = '" + BATCH_LOCK_TIMEOUT + "'");
            jdbcTemplate.execute("TRUNCATE TABLE " + qualified);
        });
        try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "TRUNCATED");
             LogDetails details = LogDetails.open()
                     .with("partition", partition)
                     .with(LogFields.ROWS, rows)) {
            // "Truncated" e non "Dropped": la partizione resta attaccata al padre, viene solo
            // svuotata. Il messaggio precedente diceva "Dropped" e faceva credere che le partizioni
            // sparissero — non spariscono, e il loro numero cresce fino alla fine della copertura.
            log.info("Truncated expired partition");
        }
        return rows;
    }

    /**
     * Cita il nome della partizione cosi' come PostgreSQL lo ha registrato.
     *
     * <p><strong>Il difetto che questo metodo esiste per impedire.</strong> Il nome arriva da
     * {@code pg_class.relname}, cioe' il nome <em>vero</em> della relazione. Le partizioni sono
     * create dalla migration con {@code format('%I', 'STG_INGEST_ERROR_' || to_char(d,'YYYYMMDD'))},
     * e {@code %I} e' {@code quote_ident}: su una stringa maiuscola aggiunge le virgolette, quindi
     * il nome conserva le maiuscole. Il padre, scritto senza virgolette nella stessa migration,
     * e' invece minuscolo.
     *
     * <p>Interpolando quel nome <em>senza</em> virgolette, PostgreSQL lo ripiega a minuscolo e la
     * relazione non esiste: in produzione il job moriva con
     * {@code relation "sert_ingestor.stg_ingest_error_20260101" does not exist} sulla prima
     * partizione scaduta, cioe' a ogni esecuzione. Conseguenza: la retention non ha mai cancellato
     * nulla da quando la migration e' andata in produzione, e la tabella e' cresciuta senza limite.
     *
     * <p>Non si corregge la migration ma il punto che costruisce l'SQL: le partizioni esistono
     * gia' col nome maiuscolo, e un nome che arriva dal catalogo va citato qualunque sia la sua
     * forma. Il raddoppio delle virgolette interne e' la stessa regola di {@code quote_ident}.
     */
    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * La DEFAULT raccoglie le righe che non trovano una partizione per la loro data. Nessun job a
     * data la svuota, quindi se non e' vuota la copertura pre-creata e' finita e va estesa.
     */
    private void warnIfDefaultPartitionHasRows(String runId) {
        try {
            Long rows = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM " + dbSchemaConfig.getSchemaName() + ".STG_INGEST_ERROR_DEFAULT",
                    Long.class);
            if (rows != null && rows > 0) {
                try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "DEFAULT_PARTITION_NOT_EMPTY");
                     LogDetails details = LogDetails.open().with(LogFields.ROWS, rows)) {
                    log.warn("Rows landed in the DEFAULT partition: daily partition coverage is "
                            + "exhausted and the next ones must be created");
                }
            }
        } catch (RuntimeException e) {
            try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "NOOP");
                 LogDetails details = LogDetails.open().with(LogFields.ERROR_DETAIL, e.getMessage())) {
                log.debug("Could not inspect the DEFAULT partition");
            }
        }
    }

    /**
     * Un batch, nella propria transazione, con i tetti applicati lato server. Il {@code SET LOCAL}
     * vale per la transazione corrente e viene ripristinato al commit, quindi non inquina la
     * connessione restituita al pool.
     */
    private int deleteBatch(String sql, OffsetDateTime threshold, int limit) {
        Integer batch = transactionTemplate.execute(status -> {
            jdbcTemplate.execute("SET LOCAL statement_timeout = '" + BATCH_STATEMENT_TIMEOUT + "'");
            jdbcTemplate.execute("SET LOCAL lock_timeout = '" + BATCH_LOCK_TIMEOUT + "'");
            return jdbcTemplate.update(sql, threshold, limit);
        });
        return batch != null ? batch : 0;
    }

    /**
     * Tetto di durata complessiva: dopo un arretrato di giorni la prima esecuzione riuscita ha
     * moltissimo da cancellare, e un job che gira per ore rischia di sovrapporsi a quello della notte
     * dopo. Si ferma e riprende alla prossima, perche' il progresso e' gia' committato.
     */
    private boolean isOverBudget(String runId, long startedNanos, Duration maxDuration, long totalDeleted) {
        if (maxDuration == null || maxDuration.isZero() || maxDuration.isNegative()) {
            return false;
        }
        long elapsedMs = elapsedMs(startedNanos);
        if (elapsedMs < maxDuration.toMillis()) {
            return false;
        }
        try (MdcScope ignored = MdcScope.run(runId, "STG_INGEST_ERROR", "BUDGET_EXCEEDED");
             LogDetails details = LogDetails.open()
                     .with("max_duration", maxDuration)
                     .with(LogFields.ELAPSED_MS, elapsedMs)
                     .with("deleted", totalDeleted)) {
            log.warn("Stopped cleanup on the duration budget, the backlog resumes on the next run");
        }
        return true;
    }

    private static long elapsedMs(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
