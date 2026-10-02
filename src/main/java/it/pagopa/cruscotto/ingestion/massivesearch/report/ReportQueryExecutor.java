package it.pagopa.cruscotto.ingestion.massivesearch.report;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * Esegue le query dei report di ricerca massiva in streaming reale, con un tetto di tempo lato server.
 *
 * <p>Risolve due problemi della lettura precedente ({@code jdbc.query} sul template condiviso):</p>
 *
 * <ul>
 *   <li><strong>fetchSize</strong>: col default di PgJDBC il driver materializza in memoria l'intero
 *   result set prima che il {@code RowCallbackHandler} scriva la prima riga del CSV. Su un report
 *   grande e' sia un rischio OOM sia tempo morto che concorre a sfondare il {@code socketTimeout}.
 *   Il cursore server-side richiede pero' <em>entrambe</em> le condizioni: {@code fetchSize > 0} e
 *   autocommit disattivato, da cui la transazione read-only esplicita — in autocommit il fetchSize
 *   viene semplicemente ignorato.</li>
 *   <li><strong>statement_timeout</strong>: {@code ingestion.persistence.statement-timeout} e' un
 *   {@code SET LOCAL} applicato solo al path di scrittura dell'ingestion, quindi queste letture
 *   giravano senza alcun tetto server-side. A scattare per primo era il {@code socketTimeout} del
 *   client, che lascia la query orfana sul server e distrugge la connessione. Con il timeout di
 *   sessione il server cancella la query e restituisce un errore pulito (SQLSTATE 57014), mantenendo
 *   la connessione riutilizzabile.</li>
 * </ul>
 *
 * <p>Il valore configurato deve restare <strong>sotto</strong> {@code socketTimeout} (360s in
 * dev/uat/prod), altrimenti il client molla prima del server e il beneficio si perde.</p>
 *
 * <p>Il template dedicato non e' quello condiviso di Spring: il {@code fetchSize} e' una proprieta'
 * del {@link JdbcTemplate}, impostarlo su quello condiviso cambierebbe il comportamento di tutto
 * l'ingestor.</p>
 */
@Slf4j
@Component
public class ReportQueryExecutor {

    private final NamedParameterJdbcTemplate streamingJdbc;
    private final TransactionTemplate transactionTemplate;
    private final long statementTimeoutMillis;

    public ReportQueryExecutor(
        DataSource dataSource,
        PlatformTransactionManager transactionManager,
        MassiveSearchProperties properties
    ) {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        template.setFetchSize(Math.max(1, properties.getExecution().getFetchSize()));
        this.streamingJdbc = new NamedParameterJdbcTemplate(template);

        TransactionTemplate tx = new TransactionTemplate(transactionManager);
        tx.setReadOnly(true);
        // I report non partecipano ad alcuna transazione chiamante: REQUIRES_NEW rende esplicito che
        // il confine e' la singola query e non eredita uno stato altrui.
        tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.transactionTemplate = tx;

        Duration timeout = properties.getExecution().getStatementTimeout();
        // Millisecondi, non secondi: statement_timeout li accetta nativamente e toSeconds() avrebbe
        // troncato a 0 — cioe' "nessun limite" — qualunque valore sotto il secondo.
        this.statementTimeoutMillis = timeout == null || timeout.isNegative() ? 0 : timeout.toMillis();
    }

    /**
     * Esegue la query in una transazione read-only con cursore server-side, invocando l'handler su
     * ogni riga via via che arriva.
     *
     * @param sql     la query gia' composta
     * @param params  i parametri nominali
     * @param handler riceve ogni riga prodotta
     */
    public void stream(String sql, SqlParameterSource params, RowCallbackHandler handler) {
        transactionTemplate.executeWithoutResult(status -> {
            applyStatementTimeout();
            streamingJdbc.query(sql, params, handler);
        });
    }

    /**
     * {@code SET LOCAL} vale per la transazione corrente e viene ripristinato al commit, quindi non
     * inquina la connessione restituita al pool. Best-effort: se fallisce si procede comunque, per
     * non far cadere un report a causa della sola rete di sicurezza.
     */
    private void applyStatementTimeout() {
        if (statementTimeoutMillis <= 0) {
            return;
        }
        try {
            streamingJdbc.getJdbcTemplate().execute("SET LOCAL statement_timeout = " + statementTimeoutMillis);
        } catch (RuntimeException e) {
            log.warn("phase=REPORT_TIMEOUT_NOT_APPLIED instanceId={} executionId={} reason={}",
                MDC.get("instanceId"), MDC.get("executionId"), e.getMessage());
        }
    }
}
