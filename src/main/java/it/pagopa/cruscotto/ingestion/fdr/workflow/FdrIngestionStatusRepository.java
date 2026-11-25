package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Lo stato operativo di {@code businessDay + dataset} dentro un batch.
 *
 * <p>È la fonte autorevole: il core non tiene lo stato in memoria e lo rilegge da qui, perché a
 * decidere se una giornata va rielaborata dovrà essere anche un processo che non l'ha mai vista —
 * il poller dei retry, un riavvio, un altro pod.
 *
 * <p>JdbcTemplate e SQL esplicito, come tutto il resto dell'ingestor. La chiave naturale è
 * {@code (BATCH_ID, BUSINESS_DAY, DATASET)} ed è protetta da un vincolo UNIQUE: una seconda
 * REINGESTION della stessa giornata vive in un batch diverso, quindi non collide con la prima.
 */
@Repository
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrIngestionStatusRepository {

    /** Come dichiarato dal changeSet: oltre, il database rifiuterebbe la riga. */
    private static final int LAST_ERROR_MESSAGE_MAX = 1000;

    private final JdbcTemplate jdbcTemplate;
    private final String schema;

    public FdrIngestionStatusRepository(JdbcTemplate jdbcTemplate, DbSchemaConfig dbSchemaConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /** Lo stato di un dataset, se il batch lo ha già registrato. */
    public Optional<FdrStatusRow> find(long batchId, LocalDate businessDay, FdrDataset dataset) {
        List<FdrStatusRow> rows = jdbcTemplate.query(
                "SELECT ID, STATUS, ATTEMPT_COUNT FROM " + schema + ".FDR_INGESTION_STATUS "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ? AND DATASET = ?",
                (rs, rowNum) -> new FdrStatusRow(rs.getLong("ID"),
                        FdrProcessingStatus.valueOf(rs.getString("STATUS")),
                        rs.getInt("ATTEMPT_COUNT")),
                batchId, java.sql.Date.valueOf(businessDay), dataset.name());
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    /**
     * Segna la presa in carico, creando la riga al primo tentativo.
     *
     * <p>{@code ATTEMPT_COUNT} si incrementa qui e non alla fine: è il numero di volte che la
     * giornata è stata <em>tentata</em>, e un tentativo che muore a metà — il pod che cade mentre
     * scrive — deve comunque risultare consumato. Contarlo all'esito lo perderebbe proprio nel caso
     * che il retry esiste per gestire.
     *
     * @return la riga dopo l'incremento, con i tentativi già consumati
     */
    public FdrStatusRow markProcessing(long batchId, LocalDate businessDay, FdrDataset dataset,
                                       Instant now) {
        FdrStatusRow existing = find(batchId, businessDay, dataset).orElse(null);
        if (existing != null) {
            jdbcTemplate.update("UPDATE " + schema + ".FDR_INGESTION_STATUS "
                            + "SET STATUS = ?, ATTEMPT_COUNT = ATTEMPT_COUNT + 1, STARTED_AT = ?, "
                            + "UPDATED_AT = ?, NEXT_ATTEMPT_AT = NULL WHERE ID = ?",
                    FdrProcessingStatus.PROCESSING.name(), Timestamp.from(now),
                    Timestamp.from(now), existing.id());
            return new FdrStatusRow(existing.id(), FdrProcessingStatus.PROCESSING,
                    existing.attemptCount() + 1);
        }
        Long id = jdbcTemplate.queryForObject(
                "INSERT INTO " + schema + ".FDR_INGESTION_STATUS "
                        + "(ID, BATCH_ID, BUSINESS_DAY, DATASET, STATUS, ATTEMPT_COUNT, "
                        + "CREATED_AT, STARTED_AT, UPDATED_AT) "
                        + "VALUES (nextval('" + schema + ".SQ_FDR_INGESTION_STATUS'), "
                        + "?, ?, ?, ?, 1, ?, ?, ?) RETURNING ID",
                Long.class, batchId, java.sql.Date.valueOf(businessDay), dataset.name(),
                FdrProcessingStatus.PROCESSING.name(), Timestamp.from(now), Timestamp.from(now),
                Timestamp.from(now));
        return new FdrStatusRow(id == null ? 0L : id, FdrProcessingStatus.PROCESSING, 1);
    }

    /**
     * Scrive l'esito del dataset.
     *
     * @param nextAttemptAt valorizzato solo per {@code WAITING_RETRY}; su un esito terminale resta
     *                      {@code null}, altrimenti il poller riprenderebbe una giornata chiusa
     */
    public void complete(long statusId, FdrDatasetResult result, Instant now,
                         Instant nextAttemptAt) {
        jdbcTemplate.update("UPDATE " + schema + ".FDR_INGESTION_STATUS "
                        + "SET STATUS = ?, ROWS_SOURCE = ?, ROWS_PROCESSED = ?, ROWS_FAILED = ?, "
                        + "LAST_ERROR_CODE = ?, LAST_ERROR_MESSAGE = ?, NEXT_ATTEMPT_AT = ?, "
                        + "COMPLETED_AT = ?, UPDATED_AT = ? WHERE ID = ?",
                result.status().name(), result.rowsSource(), result.rowsProcessed(),
                result.rowsFailed(), result.errorCode(), result.errorMessage(),
                nextAttemptAt == null ? null : Timestamp.from(nextAttemptAt),
                Timestamp.from(now), Timestamp.from(now), statusId);
    }

    /**
     * Scrive l'esito su <strong>tutti</strong> i dataset di una giornata, in un solo statement.
     *
     * <p>Serve ai percorsi che si fermano <em>prima</em> dell'elaborazione — una reingestione
     * rifiutata al preflight, un guasto tecnico mentre si prepara la giornata — e che altrimenti
     * lascerebbero le righe nello stato in cui le hanno trovate. Non e' un dettaglio di pulizia:
     * {@code FDR_INGESTION_STATUS} e' la fonte autorevole di cosa e' successo a una giornata, e una
     * riga lasciata indietro la fa sembrare ancora da fare, per sempre.
     *
     * <p>La giornata intera e non il singolo dataset, perche' quella e' l'unita': rilasciarne meta'
     * permetterebbe a due pod di lavorare sui due dataset dello stesso giorno.
     *
     * <p>Il tentativo lo consuma <strong>il database</strong>, con {@code ATTEMPT_COUNT + 1} letto
     * dalla riga stessa: una lettura seguita da scrittura, fra due pod, perderebbe un incremento. E
     * va consumato, perche' i guasti che arrivano prima dell'elaborazione non passano da
     * {@link #markProcessing}: senza, un guasto persistente verrebbe ritentato all'infinito.
     *
     * @param from  lo stato atteso: solo chi ha davvero la giornata in quello stato la muove
     * @param nextAttemptAt {@code null} su un esito terminale, altrimenti il poller la riprenderebbe
     * @return quante righe sono state aggiornate
     */
    public int settleDay(long batchId, LocalDate businessDay, FdrProcessingStatus from,
                         FdrProcessingStatus to, String errorCode, String errorMessage,
                         Instant nextAttemptAt, Instant now) {
        return jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_STATUS SET STATUS = ?, "
                        + "ATTEMPT_COUNT = ATTEMPT_COUNT + 1, NEXT_ATTEMPT_AT = ?, "
                        + "LAST_ERROR_CODE = ?, LAST_ERROR_MESSAGE = ?, UPDATED_AT = ? "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ? AND STATUS = ?",
                to.name(),
                nextAttemptAt == null ? null : Timestamp.from(nextAttemptAt),
                errorCode,
                clamp(errorMessage),
                Timestamp.from(now),
                batchId,
                java.sql.Date.valueOf(businessDay),
                from.name());
    }

    /** {@code LAST_ERROR_MESSAGE} e' VARCHAR(1000): e' un sommario, non il dettaglio del guasto. */
    private static String clamp(String message) {
        return message == null || message.length() <= LAST_ERROR_MESSAGE_MAX
                ? message
                : message.substring(0, LAST_ERROR_MESSAGE_MAX);
    }

    /** Quel poco che il core legge dello stato: identità, esito e tentativi consumati. */
    public record FdrStatusRow(long id, FdrProcessingStatus status, int attemptCount) {
    }
}
