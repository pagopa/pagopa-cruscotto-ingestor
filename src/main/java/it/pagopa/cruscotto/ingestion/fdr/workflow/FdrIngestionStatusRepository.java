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
     * Scrive l'esito del dataset.
     *
     * <p><strong>Non libera il possesso, ed e' la correzione di un difetto grave.</strong> La
     * prima stesura azzerava qui token, lease e identificativo del processo, "nella stessa UPDATE
     * dell'esito". Sembrava prudente ed era sbagliato di un livello: il possesso e' della
     * <em>giornata</em>, non del dataset. Appena il primo dei due finiva, la sua riga perdeva il
     * token; la chiusura transazionale del dataset successivo trovava <em>una</em> riga invece di
     * due e concludeva che il possesso era perso. In pratica <strong>nessuna giornata poteva
     * completare entrambi i dataset</strong>.
     *
     * <p>Nessun test con database finto poteva accorgersene — {@code renew} e' mockato e risponde
     * sempre di si' — ed e' uscito al primo giro reale contro dev: FLUSSI completo con 186.857
     * righe, IUV morto al primo batch con <em>lease renewal touched a partial day</em>.
     *
     * <p>Il rilascio ora avviene una volta sola, a giornata finita, in
     * {@link #releaseOwnership}.
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
     * Libera il possesso della giornata: una volta sola, su entrambe le righe.
     *
     * <p>Va chiamata quando la <strong>giornata</strong> e' finita, non quando finisce un dataset.
     * Il possesso e' di giornata, e rilasciarlo a meta' romperebbe la chiusura transazionale del
     * dataset ancora in lavorazione — vedi {@link #complete}, dove sta la storia per esteso.
     *
     * <p>La condizione sul token non e' difensiva, e' necessaria: se nel frattempo avessimo perso
     * il possesso, senza quella condizione staremmo azzerando il lease di chi e' subentrato.
     *
     * <p>Nessuna condizione sullo <em>stato</em>, invece: a questo punto le due righe sono gia'
     * state portate al loro esito, che puo' essere diverso fra i due dataset, e filtrare su uno
     * solo ne libererebbe una sola.
     *
     * @return quante righe sono state liberate; attese due
     */
    public int releaseOwnership(long batchId, LocalDate businessDay, String token, Instant now) {
        return jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_STATUS "
                        + "SET PROCESSING_TOKEN = NULL, LEASE_EXPIRES_AT = NULL, "
                        + "PROCESSING_INSTANCE_ID = NULL, UPDATED_AT = ? "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ? AND PROCESSING_TOKEN = ?",
                Timestamp.from(now), batchId, java.sql.Date.valueOf(businessDay), token);
    }

    /**
     * Riporta una riga all'esito che aveva <strong>prima</strong> del claim, senza toccare i
     * contatori.
     *
     * <p>Serve alla ripresa. Il claim porta in {@code PROCESSING} <em>entrambe</em> le righe della
     * giornata, anche quella di un dataset gia' concluso con successo; se poi quel dataset viene
     * saltato — ed e' giusto saltarlo, rielaborarlo ne raddoppierebbe le righe — la sua riga
     * resterebbe in lavorazione per sempre, e il recupero la ritroverebbe a ogni giro.
     *
     * <p>I contatori <strong>non</strong> vengono riscritti: quelli in colonna sono i numeri veri
     * del tentativo che ha avuto successo, e il risultato che si porta dietro la ripresa li ha a
     * zero. Sovrascriverli cancellerebbe l'unica traccia di quante righe erano state davvero
     * ingerite.
     */
    public void restorePreviousOutcome(long statusId, FdrProcessingStatus status, Instant now) {
        jdbcTemplate.update("UPDATE " + schema + ".FDR_INGESTION_STATUS "
                        + "SET STATUS = ?, NEXT_ATTEMPT_AT = NULL, UPDATED_AT = ? WHERE ID = ?",
                status.name(), Timestamp.from(now), statusId);
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
                        + "LAST_ERROR_CODE = ?, LAST_ERROR_MESSAGE = ?, UPDATED_AT = ?, "
                        + "PROCESSING_TOKEN = NULL, LEASE_EXPIRES_AT = NULL, "
                        + "PROCESSING_INSTANCE_ID = NULL "
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

    /**
     * Chiude una giornata <strong>incoerente</strong>, senza condizione sullo stato di partenza.
     *
     * <p>È l'unico metodo che tocca le righe senza chiedersi in che stato fossero, e la ragione è
     * esattamente l'anomalia che sta chiudendo: se le due righe non concordano sul tentativo,
     * possono benissimo non concordare nemmeno sullo stato, e un {@code WHERE … AND STATUS = ?} ne
     * chiuderebbe <em>una sola</em> — lasciando la giornata metà chiusa e metà in attesa, cioè
     * peggio di come l'ha trovata.
     *
     * <p><strong>Il tentativo non viene incrementato.</strong> Consumarne uno su un conteggio di cui
     * si è appena stabilito che non è affidabile aggiungerebbe un numero sbagliato a un numero
     * sbagliato. Lo stato diventa {@code FAILED} e {@code NEXT_ATTEMPT_AT} resta nullo: la giornata
     * non torna al poller, e per rifarla serve una REINGESTION — cioè una decisione presa da
     * qualcuno, che è il punto.
     *
     * @return quante righe sono state chiuse; atteso il numero dei dataset
     */
    public int settleInconsistentDay(long batchId, LocalDate businessDay, String errorCode,
                                     String errorMessage, Instant now) {
        return jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_STATUS SET STATUS = ?, "
                        + "NEXT_ATTEMPT_AT = NULL, LAST_ERROR_CODE = ?, LAST_ERROR_MESSAGE = ?, "
                        + "COMPLETED_AT = ?, UPDATED_AT = ?, PROCESSING_TOKEN = NULL, "
                        + "LEASE_EXPIRES_AT = NULL, PROCESSING_INSTANCE_ID = NULL "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ?",
                FdrProcessingStatus.FAILED.name(), errorCode, clamp(errorMessage),
                Timestamp.from(now), Timestamp.from(now), batchId,
                java.sql.Date.valueOf(businessDay));
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
