package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Le richieste di elaborazione: {@code FDR_INGESTION_BATCH}.
 *
 * <p>E' il punto di incontro fra backend e Ingestor. Il backend <strong>scrive una riga</strong> e
 * se ne va; l'Ingestor la trova col polling, se la prende e la esegue. Nessuna chiamata sincrona
 * fra i due, quindi nessuno dei due deve essere su per far funzionare l'altro.
 */
@Repository
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrBatchRepository {

    /**
     * Stato di una richiesta appena creata, in attesa che un worker la prenda.
     *
     * <p>Deriva dall'enum e non e' una stringa a se': scriverla due volte significherebbe poterne
     * cambiare una sola, e il polling smetterebbe di trovare le richieste senza che nulla fallisca.
     */
    public static final String PENDING = FdrProcessingStatus.PENDING.name();

    private final JdbcTemplate jdbcTemplate;
    private final String schema;

    public FdrBatchRepository(JdbcTemplate jdbcTemplate, DbSchemaConfig dbSchemaConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /**
     * Crea un batch gia' in lavorazione.
     *
     * <p>Lo usa solo il DAILY, che non passa dal polling: se lo creasse {@code PENDING} il command
     * poller potrebbe prenderlo, e il DAILY diventerebbe una richiesta amministrativa come le
     * altre. Nasce quindi gia' {@code PROCESSING}, di proprieta' di chi lo ha creato.
     *
     * @param from estremi della richiesta; per il DAILY sono entrambi la giornata stessa
     */
    public long createProcessing(FdrWorkflow workflow, LocalDate from, LocalDate to,
                                 String requestedBy, Instant now) {
        Long id = jdbcTemplate.queryForObject(
                "INSERT INTO " + schema + ".FDR_INGESTION_BATCH "
                        + "(ID, WORKFLOW, STATUS, REQUESTED_FROM, REQUESTED_TO, REQUESTED_BY, "
                        + "CREATED_AT, STARTED_AT, UPDATED_AT) "
                        + "VALUES (nextval('" + schema + ".SQ_FDR_INGESTION_BATCH'), "
                        + "?, ?, ?, ?, ?, ?, ?, ?) RETURNING ID",
                Long.class, workflow.name(), FdrProcessingStatus.PROCESSING.name(),
                from == null ? null : Date.valueOf(from), to == null ? null : Date.valueOf(to),
                requestedBy, Timestamp.from(now), Timestamp.from(now), Timestamp.from(now));
        return id == null ? 0L : id;
    }

    /** Le richieste amministrative in attesa, piu' vecchia per prima. */
    public List<FdrBatchRow> findPendingCommands() {
        return jdbcTemplate.query(
                "SELECT ID, WORKFLOW, REQUESTED_FROM, REQUESTED_TO FROM " + schema
                        + ".FDR_INGESTION_BATCH WHERE STATUS = ? AND WORKFLOW IN (?, ?) "
                        + "ORDER BY CREATED_AT, ID",
                (rs, rowNum) -> new FdrBatchRow(rs.getLong("ID"),
                        FdrWorkflow.valueOf(rs.getString("WORKFLOW")),
                        toLocalDate(rs.getDate("REQUESTED_FROM")),
                        toLocalDate(rs.getDate("REQUESTED_TO"))),
                PENDING, FdrWorkflow.BOOTSTRAP.name(), FdrWorkflow.REINGESTION.name());
    }

    /**
     * Prende in carico una richiesta, se nessun altro l'ha gia' presa.
     *
     * <p>E' una sola {@code UPDATE} condizionata sullo stato: PostgreSQL blocca la riga per la
     * durata dello statement, quindi fra due pod che tentano insieme <strong>uno solo</strong>
     * vede {@code 1} riga aggiornata e l'altro vede {@code 0}. Non serve altro: niente lock
     * applicativi, che non attraverserebbero i pod, e niente {@code SELECT ... FOR UPDATE}, che
     * qui aggiungerebbe un giro senza aggiungere garanzie.
     *
     * @return {@code true} se il batch e' nostro
     */
    public boolean claim(long batchId, Instant now) {
        int updated = jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_BATCH SET STATUS = ?, STARTED_AT = ?, "
                        + "UPDATED_AT = ? WHERE ID = ? AND STATUS = ?",
                FdrProcessingStatus.PROCESSING.name(), Timestamp.from(now), Timestamp.from(now),
                batchId, PENDING);
        return updated == 1;
    }

    /**
     * Le richieste amministrative prese in carico e mai concluse.
     *
     * <p>Il filtro temporale e' una condizione <em>necessaria</em>, non sufficiente: dice solo che
     * la richiesta e' li' da abbastanza tempo da meritare una domanda. A rispondere e'
     * {@link FdrStaleProcessingDetector}, guardando se esiste ancora un'esecuzione FDR che batte.
     * Da sola, una soglia dichiarerebbe morta una giornata grande ancora in corso.
     */
    public List<FdrBatchRow> findStaleProcessing(Instant notUpdatedSince) {
        return jdbcTemplate.query(
                "SELECT ID, WORKFLOW, REQUESTED_FROM, REQUESTED_TO FROM " + schema
                        + ".FDR_INGESTION_BATCH WHERE STATUS = ? AND WORKFLOW IN (?, ?) "
                        + "AND UPDATED_AT < ? ORDER BY CREATED_AT, ID",
                (rs, rowNum) -> new FdrBatchRow(rs.getLong("ID"),
                        FdrWorkflow.valueOf(rs.getString("WORKFLOW")),
                        toLocalDate(rs.getDate("REQUESTED_FROM")),
                        toLocalDate(rs.getDate("REQUESTED_TO"))),
                FdrProcessingStatus.PROCESSING.name(), FdrWorkflow.BOOTSTRAP.name(),
                FdrWorkflow.REINGESTION.name(), Timestamp.from(notUpdatedSince));
    }

    /**
     * Rimette una richiesta abbandonata in coda, invece di eseguirla al volo.
     *
     * <p>Cosi' il recupero non ha un percorso proprio: la richiesta torna {@code PENDING} e viene
     * ripresa dal polling normale, claim compreso. Un percorso separato sarebbe la seconda
     * implementazione della stessa cosa — e quella meno esercitata delle due.
     *
     * <p>Condizionata su {@code PROCESSING} per la stessa ragione del claim: fra due pod che
     * decidono insieme, uno solo rilascia.
     *
     * @return {@code true} se e' stata rimessa in coda da questa chiamata
     */
    public boolean releaseToPending(long batchId, Instant now) {
        int updated = jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_BATCH SET STATUS = ?, UPDATED_AT = ? "
                        + "WHERE ID = ? AND STATUS = ?",
                PENDING, Timestamp.from(now), batchId, FdrProcessingStatus.PROCESSING.name());
        return updated == 1;
    }

    /** Scrive l'esito complessivo della richiesta. */
    public void complete(long batchId, FdrProcessingStatus status, Instant now) {
        jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_BATCH SET STATUS = ?, COMPLETED_AT = ?, "
                        + "UPDATED_AT = ? WHERE ID = ?",
                status.name(), Timestamp.from(now), Timestamp.from(now), batchId);
    }

    /**
     * Le giornate di una richiesta, dagli status e non dagli estremi.
     *
     * <p>E' la differenza che rende rappresentabile una reingestion sparsa: il 10/10, il 15/11 e
     * il 10/12 sono tre righe, mentre {@code REQUESTED_FROM..REQUESTED_TO} sarebbero due mesi.
     */
    public List<LocalDate> findBusinessDays(long batchId) {
        return jdbcTemplate.queryForList(
                "SELECT DISTINCT BUSINESS_DAY FROM " + schema + ".FDR_INGESTION_STATUS "
                        + "WHERE BATCH_ID = ? ORDER BY BUSINESS_DAY",
                Date.class, batchId).stream().map(Date::toLocalDate).toList();
    }

    /** I dataset registrati per una giornata, usati per validare la richiesta. */
    public List<String> findDatasets(long batchId, LocalDate businessDay) {
        return jdbcTemplate.queryForList(
                "SELECT DATASET FROM " + schema + ".FDR_INGESTION_STATUS "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ? ORDER BY DATASET",
                String.class, batchId, Date.valueOf(businessDay));
    }

    /**
     * Registra una giornata nel batch, se non c'e' gia'.
     *
     * <p>Serve al BOOTSTRAP, che riceve un intervallo e lo espande dopo il claim. Il vincolo
     * UNIQUE su {@code (BATCH_ID, BUSINESS_DAY, DATASET)} rende l'operazione ripetibile: una
     * ripresa dopo un crash non duplica le righe.
     */
    public void createStatusIfAbsent(long batchId, LocalDate businessDay, FdrDataset dataset,
                                     Instant now) {
        jdbcTemplate.update(
                "INSERT INTO " + schema + ".FDR_INGESTION_STATUS "
                        + "(ID, BATCH_ID, BUSINESS_DAY, DATASET, STATUS, ATTEMPT_COUNT, "
                        + "CREATED_AT, UPDATED_AT) "
                        + "SELECT nextval('" + schema + ".SQ_FDR_INGESTION_STATUS'), ?, ?, ?, ?, 0, ?, ? "
                        + "WHERE NOT EXISTS (SELECT 1 FROM " + schema + ".FDR_INGESTION_STATUS "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ? AND DATASET = ?)",
                new Object[] {batchId, Date.valueOf(businessDay), dataset.name(), PENDING,
                        Timestamp.from(now), Timestamp.from(now),
                        batchId, Date.valueOf(businessDay), dataset.name()},
                new int[] {Types.BIGINT, Types.DATE, Types.VARCHAR, Types.VARCHAR,
                        Types.TIMESTAMP_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE,
                        Types.BIGINT, Types.DATE, Types.VARCHAR});
    }

    /**
     * Gli esiti dei dataset di un batch, per aggregare lo stato complessivo.
     *
     * <p><strong>I valori non riconosciuti restano nella lista come {@code null}</strong>, e non
     * vengono scartati: {@link #aggregate} li legge come "non terminale", che e' l'unica lettura
     * prudente. Filtrarli — come faceva la prima stesura — faceva sparire dall'aggregazione proprio
     * le righe di cui non si sa nulla, e un batch a cui mancava una giornata all'appello risultava
     * COMPLETED.
     */
    public List<FdrProcessingStatus> findStatuses(long batchId) {
        return jdbcTemplate.queryForList(
                "SELECT STATUS FROM " + schema + ".FDR_INGESTION_STATUS WHERE BATCH_ID = ?",
                String.class, batchId).stream().map(FdrBatchRepository::toStatus).toList();
    }

    /** Se esiste gia' un DAILY per quella giornata, con il suo esito piu' avanzato. */
    public Optional<FdrProcessingStatus> findDailyOutcome(LocalDate businessDay) {
        List<String> statuses = jdbcTemplate.queryForList(
                "SELECT s.STATUS FROM " + schema + ".FDR_INGESTION_STATUS s "
                        + "JOIN " + schema + ".FDR_INGESTION_BATCH b ON b.ID = s.BATCH_ID "
                        + "WHERE b.WORKFLOW = ? AND s.BUSINESS_DAY = ?",
                String.class, FdrWorkflow.DAILY.name(), Date.valueOf(businessDay));
        if (statuses.isEmpty()) {
            return Optional.empty();
        }
        // Lo stato della giornata e' quello dei suoi dataset composti con la stessa regola del
        // core: cosi' la guardia di idempotenza e l'esito scritto dicono la stessa cosa.
        return Optional.of(aggregate(statuses.stream().map(FdrBatchRepository::toStatus).toList()));
    }

    static FdrProcessingStatus aggregate(List<FdrProcessingStatus> statuses) {
        // PENDING pesa come PROCESSING: una giornata registrata e mai presa non ha un esito, e
        // ometterla farebbe risultare COMPLETED un batch in cui manca all'appello.
        if (statuses.stream().anyMatch(s -> s == null || s == FdrProcessingStatus.PENDING
                || s == FdrProcessingStatus.PROCESSING)) {
            return FdrProcessingStatus.PROCESSING;
        }
        if (statuses.stream().anyMatch(s -> s == FdrProcessingStatus.WAITING_RETRY)) {
            return FdrProcessingStatus.WAITING_RETRY;
        }
        if (statuses.stream().anyMatch(s -> s == FdrProcessingStatus.FAILED
                || s == FdrProcessingStatus.MISSING)) {
            return FdrProcessingStatus.FAILED;
        }
        if (statuses.stream().anyMatch(s -> s == FdrProcessingStatus.COMPLETED_WITH_WARNING)) {
            return FdrProcessingStatus.COMPLETED_WITH_WARNING;
        }
        return FdrProcessingStatus.COMPLETED;
    }

    /**
     * {@code null} per una stringa che l'enum non conosce.
     *
     * <p>Non puo' succedere per gli stati che scriviamo noi, ma la colonna non ha un CHECK: se un
     * domani qualcuno ci scrivesse altro, preferiamo aggregare "non terminale" piuttosto che far
     * fallire la chiusura del batch.
     */
    private static FdrProcessingStatus toStatus(String value) {
        try {
            return FdrProcessingStatus.valueOf(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static LocalDate toLocalDate(Date date) {
        return date == null ? null : date.toLocalDate();
    }

    /** Quel poco che serve di una richiesta per deciderne il destino. */
    public record FdrBatchRow(long id, FdrWorkflow workflow, LocalDate requestedFrom,
                              LocalDate requestedTo) {
    }
}
