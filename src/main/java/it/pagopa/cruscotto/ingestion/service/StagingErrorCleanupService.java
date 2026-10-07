package it.pagopa.cruscotto.ingestion.service;

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
            log.info("[runId={}][entityName=STG_INGEST_ERROR][phase=NOOP] cleanup disabled", runId);
            return 0;
        }

        OffsetDateTime threshold = OffsetDateTime.now(ZoneOffset.UTC).minus(config.getRetention());
        // La tabella e' partizionata per giorno dalla migration 47, ma il codice non assume che la
        // migration sia gia' passata: se non lo e' (deploy del codice prima della migration, o
        // ambiente locale non aggiornato) si ricade sulla DELETE a batch, che resta corretta.
        if (isPartitioned()) {
            return truncateExpiredPartitions(runId, threshold);
        }
        log.info("[runId={}][entityName=STG_INGEST_ERROR][phase=FALLBACK] tabella non partizionata, "
                + "retention applicata con DELETE a batch", runId);
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

        log.info("[runId={}][entityName=STG_INGEST_ERROR][phase=END] retention={} threshold={} "
                        + "batchSize={} deleted={} elapsedMs={} exhausted={}",
                runId, config.getRetention(), threshold, limit, totalDeleted,
                elapsedMs(startedNanos), exhausted);
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
        log.info("[runId={}][entityName=STG_INGEST_ERROR][phase=END] threshold={} partitions={} removed={}",
                runId, threshold, expired.size(), removed);
        return removed;
    }

    /** Svuota una partizione, registrando quanti record non riconciliati vanno perduti. */
    private long truncatePartition(String runId, String partition) {
        String qualified = dbSchemaConfig.getSchemaName() + "." + partition;
        Long pending = transactionTemplate.execute(status -> jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + qualified + " WHERE STATUS <> 'DONE'", Long.class));
        Long total = transactionTemplate.execute(status -> jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + qualified, Long.class));
        long rows = total != null ? total : 0;
        if (rows == 0) {
            return 0;
        }
        if (pending != null && pending > 0) {
            log.warn("[runId={}][entityName=STG_INGEST_ERROR][phase=DATA_LOSS] partition={} "
                            + "recordNonRiconciliati={} su={} — superata la retention senza essere recuperati",
                    runId, partition, pending, rows);
        }
        transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.execute("SET LOCAL lock_timeout = '" + BATCH_LOCK_TIMEOUT + "'");
            jdbcTemplate.execute("TRUNCATE TABLE " + qualified);
        });
        log.info("[runId={}][entityName=STG_INGEST_ERROR][phase=TRUNCATED] partition={} rows={}",
                runId, partition, rows);
        return rows;
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
                log.warn("[runId={}][entityName=STG_INGEST_ERROR][phase=DEFAULT_PARTITION_NOT_EMPTY] rows={} "
                                + "— la copertura delle partizioni giornaliere e' esaurita: vanno create le successive",
                        runId, rows);
            }
        } catch (RuntimeException e) {
            log.debug("[runId={}][entityName=STG_INGEST_ERROR] partizione DEFAULT non ispezionabile: {}",
                    runId, e.getMessage());
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
        log.warn("[runId={}][entityName=STG_INGEST_ERROR][phase=BUDGET_EXCEEDED] maxDuration={} "
                        + "elapsedMs={} deleted={} — resta dell'arretrato, riprende alla prossima esecuzione",
                runId, maxDuration, elapsedMs, totalDeleted);
        return true;
    }

    private static long elapsedMs(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
