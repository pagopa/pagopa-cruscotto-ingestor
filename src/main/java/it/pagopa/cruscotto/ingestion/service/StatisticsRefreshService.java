package it.pagopa.cruscotto.ingestion.service;

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
        IngestionConfig.StatisticsRefreshConfig config = ingestionConfig.getStatisticsRefresh();
        if (!config.isEnabled()) {
            log.info("[runId={}][entityName=STATISTICS_REFRESH][phase=NOOP] disabilitato da configurazione", runId);
            return 0;
        }

        List<String> tables = config.getTables();
        if (tables == null || tables.isEmpty()) {
            log.warn("[runId={}][entityName=STATISTICS_REFRESH][phase=NOOP] nessuna tabella configurata", runId);
            return 0;
        }

        String timeout = timeoutLiteral(config.getStatementTimeout());
        long startedNanos = System.nanoTime();
        int analyzed = 0;
        int failed = 0;

        for (String table : tables) {
            if (!SAFE_IDENTIFIER.matcher(table).matches()) {
                // Non e' un errore recuperabile: e' una configurazione sbagliata, e va vista.
                log.error("[runId={}][entityName=STATISTICS_REFRESH][phase=ERROR] nome tabella non valido, "
                        + "ignorato: {}", runId, table);
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
                log.info("[runId={}][entityName=STATISTICS_REFRESH][phase=ANALYZED] table={} elapsedMs={}",
                        runId, qualified, elapsedMs(tableStartedNanos));
            } catch (CannotAcquireLockException exception) {
                // Atteso e transitorio: autovacuum o una retention tenevano la tabella. Si riprende
                // alla prossima esecuzione, quindi non e' un errore da svegliare qualcuno.
                failed++;
                log.warn("[runId={}][entityName=STATISTICS_REFRESH][phase=SKIPPED] table={} elapsedMs={} "
                                + "lock non ottenuto entro {} (autovacuum o retention sulla stessa tabella): "
                                + "si riprova alla prossima esecuzione",
                        runId, qualified, elapsedMs(tableStartedNanos), LOCK_TIMEOUT);
            } catch (QueryTimeoutException exception) {
                // NON si risolve da solo: se ANALYZE non sta nel tetto, non ci stara' nemmeno domani.
                // Le statistiche di questa tabella restano stantie e il planner continua a sbagliare i
                // piani, in silenzio. Il log deve dire la leva, altrimenti il problema si vede solo
                // come query lente settimane dopo.
                failed++;
                log.error("[runId={}][entityName=STATISTICS_REFRESH][phase=TIMEOUT] table={} elapsedMs={} "
                                + "statementTimeout={} — ANALYZE non completato: le statistiche di questa "
                                + "tabella restano STANTIE e il problema si ripresentera' a ogni esecuzione. "
                                + "Alzare ingestion.statistics-refresh.statement-timeout "
                                + "(INGESTION_STATISTICS_REFRESH_STATEMENT_TIMEOUT)",
                        runId, qualified, elapsedMs(tableStartedNanos), timeout);
            } catch (Exception exception) {
                // Una tabella che fallisce non deve impedire le altre: le statistiche di ciascuna sono
                // indipendenti, e averne quattro su cinque aggiornate e' meglio di zero.
                failed++;
                log.error("[runId={}][entityName=STATISTICS_REFRESH][phase=ERROR] table={} elapsedMs={} error={}",
                        runId, qualified, elapsedMs(tableStartedNanos), exception.getMessage());
            }
        }

        log.info("[runId={}][entityName=STATISTICS_REFRESH][phase=END] analyzed={} failed={} elapsedMs={}",
                runId, analyzed, failed, elapsedMs(startedNanos));
        return analyzed;
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

    private static long elapsedMs(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
