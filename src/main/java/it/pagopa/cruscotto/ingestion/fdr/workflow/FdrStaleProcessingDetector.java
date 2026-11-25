package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * Dice se una giornata ferma su {@code PROCESSING} e' ancora in lavorazione o e' stata abbandonata.
 *
 * <p>La domanda si pone perche' uno stato {@code PROCESSING} ha due significati opposti: un pod che
 * sta lavorando, o un pod morto a meta'. Distinguerli con un timeout arbitrario — "piu' di dieci
 * minuti, quindi e' morto" — significherebbe dichiarare morta una giornata grande ancora viva.
 *
 * <p>La prova la da' l'AS-IS: ogni job scrive in {@code INGEST_EXECUTION_LOG} una riga con un
 * <strong>heartbeat</strong>, e un reaper la chiude come {@code ABORTED} quando il battito si
 * ferma. Qui si guarda la stessa cosa: se esiste un'esecuzione FDR viva, la giornata non e'
 * abbandonata. Non e' una stima, e' un'osservazione.
 */
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrStaleProcessingDetector {

    private final JdbcTemplate jdbcTemplate;
    private final String schema;
    private final Duration staleAfter;

    public FdrStaleProcessingDetector(JdbcTemplate jdbcTemplate, DbSchemaConfig dbSchemaConfig,
                                      FdrProperties fdrProperties) {
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
        this.staleAfter = fdrProperties.getRecovery().getStaleAfter();
    }

    /**
     * Se un'esecuzione FDR sta battendo in questo momento.
     *
     * <p>Granularita' volutamente grossolana: basta <em>una</em> esecuzione FDR viva per non
     * toccare nulla. Correlare la singola giornata al suo pod richiederebbe di scrivere l'instance
     * id sullo status, cioe' duplicare un'informazione che l'execution log gia' tiene, e la
     * grossolanita' sbaglia nella direzione sicura: al massimo si aspetta un giro in piu' prima di
     * recuperare, mai si recupera una giornata che sta ancora lavorando.
     */
    public boolean isAnyFdrExecutionAlive(Instant now) {
        Long alive = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM " + schema + ".INGEST_EXECUTION_LOG "
                        + "WHERE ENTITY_NAME LIKE 'FDR%' AND STATUS = 'STARTED' "
                        + "AND LAST_HEARTBEAT_AT > ?",
                Long.class, Timestamp.from(now.minus(staleAfter)));
        return alive != null && alive > 0;
    }

    /**
     * Se una giornata presa in carico va considerata abbandonata.
     *
     * @param startedAt quando lo status e' passato in {@code PROCESSING}
     */
    public boolean isStale(Instant startedAt, Instant now) {
        if (startedAt == null) {
            // Senza un istante di presa in carico non c'e' evidenza di nulla: non si recupera.
            return false;
        }
        return startedAt.isBefore(now.minus(staleAfter)) && !isAnyFdrExecutionAlive(now);
    }
}
