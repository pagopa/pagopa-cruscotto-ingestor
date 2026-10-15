package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * JDBC access to {@code search_execution} lifecycle rows.
 */
@Repository
public class SearchExecutionRepository {

    private static final int ERROR_CODE_MAX_LENGTH = 128;
    private static final String STUCK_ERROR_CODE = "STUCK_EXECUTION";
    private static final String STUCK_ERROR_MESSAGE = "Execution exceeded running timeout";

    /**
     * Tetto alle esecuzioni recuperate in una singola scansione.
     *
     * <p>Ogni recupero apre ora una transazione propria. Senza tetto, un arretrato anomalo — una
     * finestra di indisponibilita' del database, o un timeout configurato per sbaglio a un valore
     * minuscolo — farebbe aprire e chiudere centinaia di transazioni sul <strong>thread di Quartz
     * che fa la scansione</strong>, bloccando nel frattempo la raccolta di nuove istanze.
     *
     * <p>Le esecuzioni oltre il tetto non si perdono: restano RUNNING e la scansione successiva,
     * fra cinque minuti, le ritrova. L'{@code ORDER BY} sul segno di vita fa recuperare prima le
     * piu' vecchie, cosi' l'arretrato si smaltisce in ordine invece che a caso.
     */
    private static final int MAX_RECOVERED_PER_SCAN = 200;

    private final NamedParameterJdbcTemplate jdbc;
    private final String schema;

    public SearchExecutionRepository(NamedParameterJdbcTemplate jdbc, DbSchemaConfig dbSchemaConfig) {
        this.jdbc = jdbc;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /** Inserts a new execution in {@code PENDING} state and returns its id. */
    public UUID insertPending(UUID instanceId) {
        UUID id = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "INSERT INTO " + schema + ".search_execution"
            + " (id, instance_id, status, created_at, updated_at)"
            + " VALUES (:id, :instanceId, :status, :now, :now)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", id)
            .addValue("instanceId", instanceId)
            .addValue("status", ExecutionStatus.PENDING.name())
            .addValue("now", now);
        jdbc.update(sql, params);
        return id;
    }

    public void markRunning(UUID executionId) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET status = :status, started_at = :now, updated_at = :now"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("status", ExecutionStatus.RUNNING.name())
            .addValue("now", now);
        jdbc.update(sql, params);
    }

    public void markCompleted(UUID executionId, long totalInputRows, long processedRows, int generatedFiles) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET status = :status, completed_at = :now, updated_at = :now,"
            + " total_input_rows = :totalInputRows, processed_rows = :processedRows, generated_files = :generatedFiles"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("status", ExecutionStatus.COMPLETED.name())
            .addValue("now", now)
            .addValue("totalInputRows", totalInputRows)
            .addValue("processedRows", processedRows)
            .addValue("generatedFiles", generatedFiles);
        jdbc.update(sql, params);
    }

    public void markFailed(UUID executionId, String errorCode, String errorMessage) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET status = :status, completed_at = :now, updated_at = :now,"
            + " error_code = :errorCode, error_message = :errorMessage"
            + " WHERE id = :id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("status", ExecutionStatus.FAILED.name())
            .addValue("now", now)
            .addValue("errorCode", truncate(errorCode))
            .addValue("errorMessage", errorMessage);
        jdbc.update(sql, params);
    }

    private String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() <= ERROR_CODE_MAX_LENGTH ? value : value.substring(0, ERROR_CODE_MAX_LENGTH);
    }

    /**
     * Limita, per la transazione corrente, l'attesa su un lock e la durata dello statement.
     *
     * <p>Il recupero gira sul thread di Quartz che fa la scansione: senza un tetto, un
     * {@code UPDATE} che incontra una riga bloccata da un'altra transazione aspetterebbe
     * <strong>per sempre</strong>, e con lui si fermerebbe l'intero polling della ricerca massiva —
     * nessuna istanza nuova verrebbe piu' raccolta. E' lo stesso tetto gia' applicato dal reaper
     * dell'ingestion in {@code ExecutionLogService#markOrphanedRunsAborted}.</p>
     *
     * <p>{@code SET LOCAL} ha effetto <strong>solo dentro una transazione</strong>: fuori, PostgreSQL
     * emette un warning e non applica nulla. Va quindi invocato dall'interno della transazione di
     * recupero, mai come preludio a un'operazione in autocommit.</p>
     *
     * <p>Best-effort: se fallisce si procede comunque, perche' non poter restringere un timeout non
     * e' una ragione per rinunciare al recupero.</p>
     */
    public void applyRecoveryTimeouts() {
        try {
            jdbc.getJdbcTemplate().execute("SET LOCAL lock_timeout = '10s'");
            jdbc.getJdbcTemplate().execute("SET LOCAL statement_timeout = '30s'");
        } catch (RuntimeException ignored) {
            // si procede senza tetti locali
        }
    }

    /**
     * Registra un segno di vita dell'esecuzione, cosi' che il recoverer non la scambi per bloccata.
     *
     * <p>Condizionato a {@code status = RUNNING}: su un'esecuzione gia' terminata — o gia'
     * recuperata da un altro pod — non deve riaprire nulla, deve semplicemente non aggiornare
     * niente. {@code updated_at} <strong>non</strong> viene toccato di proposito: segna le
     * transizioni di stato, e farlo avanzare a ogni battito renderebbe illeggibile il momento in
     * cui l'esecuzione e' davvero cambiata.</p>
     *
     * @return {@code true} se l'esecuzione era RUNNING e il battito e' stato registrato
     */
    public boolean heartbeat(UUID executionId) {
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET last_heartbeat_at = :now WHERE id = :id AND status = :running";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("now", OffsetDateTime.now())
            .addValue("running", ExecutionStatus.RUNNING.name());
        return jdbc.update(sql, params) == 1;
    }

    /**
     * Finds executions still {@code RUNNING} whose last sign of life is older than {@code threshold}
     * (i.e. stuck: the JVM/pod likely died mid-processing).
     *
     * <p><strong>La soglia si misura sull'ultimo battito, non su {@code started_at}.</strong> Una
     * scadenza fissa sull'istante di avvio non distingue un'esecuzione morta da una viva e lenta:
     * un report su un perimetro grande e' una sequenza di centinaia di query che puo' superare
     * {@code running-timeout-minutes} pur avanzando regolarmente, e veniva marcato FAILED mentre
     * stava ancora lavorando — lasciando una riga FAILED e un'esecuzione viva sul thread.</p>
     *
     * <p>La {@code COALESCE} copre le esecuzioni nate prima dell'introduzione della colonna e quelle
     * morte prima del primo battito: per entrambe vale ancora {@code started_at}, cioe' il
     * comportamento precedente.</p>
     */
    public List<StuckExecutionRef> findStuckRunningExecutions(OffsetDateTime threshold) {
        String sql = "SELECT id, instance_id FROM " + schema + ".search_execution"
            + " WHERE status = :status AND started_at IS NOT NULL"
            + " AND COALESCE(last_heartbeat_at, started_at) < :threshold"
            + " ORDER BY COALESCE(last_heartbeat_at, started_at) LIMIT :limit";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("status", ExecutionStatus.RUNNING.name())
            .addValue("threshold", threshold)
            .addValue("limit", MAX_RECOVERED_PER_SCAN);
        return jdbc.query(sql, params, (rs, n) ->
            new StuckExecutionRef(rs.getObject("id", UUID.class), rs.getObject("instance_id", UUID.class)));
    }

    /**
     * Recovery-only transition: marks a stuck execution {@code FAILED} with the
     * {@code STUCK_EXECUTION} error code, but only if it is still {@code RUNNING} (so a normal
     * completion happening concurrently is never overwritten).
     *
     * @return {@code true} when the execution was RUNNING and got marked FAILED
     */
    public boolean recoverStuck(UUID executionId) {
        OffsetDateTime now = OffsetDateTime.now();
        String sql = "UPDATE " + schema + ".search_execution"
            + " SET status = :failed, completed_at = :now, updated_at = :now,"
            + " error_code = :errorCode, error_message = :errorMessage"
            + " WHERE id = :id AND status = :running";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("id", executionId)
            .addValue("failed", ExecutionStatus.FAILED.name())
            .addValue("running", ExecutionStatus.RUNNING.name())
            .addValue("now", now)
            .addValue("errorCode", STUCK_ERROR_CODE)
            .addValue("errorMessage", STUCK_ERROR_MESSAGE);
        return jdbc.update(sql, params) == 1;
    }
}
