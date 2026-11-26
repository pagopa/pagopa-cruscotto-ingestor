package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

/**
 * Il registro di cosa è successo a ogni file: {@code FDR_INGESTION_FILE}.
 *
 * <p>Esiste perché il cliente ha deciso di <strong>non</strong> avere staging business FDR. Senza
 * staging le tabelle business si popolano progressivamente e una giornata può essere parziale
 * mentre viene elaborata; questo log è ciò che rende quella situazione leggibile — quali file erano
 * stati iniziati, quali chiusi, con che conteggi.
 *
 * <p>La forma viene da {@code search_execution_step} della ricerca massiva, che fa la stessa cosa
 * per le fasi di una ricerca: {@link #begin} apre una riga in lavorazione, {@link #complete} o
 * {@link #fail} la chiudono con <strong>un solo</strong> UPDATE. La durata è calcolata dal database
 * a partire da {@code STARTED_AT}, non dall'orologio del pod: su tre pod gli orologi non sono lo
 * stesso orologio, e una durata negativa in colonna non si spiega a nessuno.
 *
 * <p><strong>Un solo UPDATE per chiusura, non due.</strong> Scrivere prima i contatori e poi lo
 * stato terminale significherebbe che il secondo statement può non essere mai eseguito, lasciando
 * una riga chiusa senza numeri o con numeri senza esito — e proprio nei casi che interessa
 * analizzare, perché è quando qualcosa va storto che il secondo statement salta.
 *
 * <p><strong>Non è uno staging e non è un checkpoint.</strong> Non contiene dato business, e uno
 * stato {@code COMPLETED} non autorizza nessuno a saltare quel file in un tentativo successivo: la
 * ripresa continua ad avere la giornata intera come unità.
 */
@Repository
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrIngestionFileRepository {

    /** Le due colonne diagnostiche sono VARCHAR: qui accorciare e' legittimo, e' testo nostro. */
    private static final int ERROR_CODE_MAX = 128;
    private static final int ERROR_MESSAGE_MAX = 1000;

    private final JdbcTemplate jdbcTemplate;
    private final String schema;

    public FdrIngestionFileRepository(JdbcTemplate jdbcTemplate, DbSchemaConfig dbSchemaConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /**
     * Apre la riga del file, <strong>prima</strong> che la lettura cominci.
     *
     * <p>Aprirla dopo renderebbe invisibile l'unico caso per cui serve: un file iniziato e mai
     * finito perché il pod è morto a metà. I metadata vengono tutti da {@link FdrBlobFile}, cioè
     * dalla discovery già fatta: nessuna chiamata ad Azure in più.
     *
     * @param attemptNo il tentativo della giornata, da {@code FDR_INGESTION_STATUS.ATTEMPT_COUNT}.
     *                  Serve a distinguere due letture dello stesso blob nello stesso batch, non a
     *                  decidere la politica di retry
     * @return l'identificativo della riga, da passare alla chiusura
     */
    public long begin(long batchId, Long statusId, FdrBlobFile file, int attemptNo, Instant now) {
        Long id = jdbcTemplate.queryForObject(
                "INSERT INTO " + schema + ".FDR_INGESTION_FILE "
                        + "(ID, BATCH_ID, STATUS_ID, BUSINESS_DAY, DATASET, BLOB_NAME, PROGRESSIVE, "
                        + "FILE_SIZE, ETAG, STATUS, ATTEMPT_NO, ROWS_SOURCE, ROWS_READ, "
                        + "ROWS_PROCESSED, ROWS_FAILED, ROWS_TRUNCATED, STARTED_AT, CREATED_AT, "
                        + "UPDATED_AT) "
                        + "VALUES (nextval('" + schema + ".SQ_FDR_INGESTION_FILE'), "
                        + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, 0, 0, ?, ?, ?) RETURNING ID",
                new Object[] {
                        batchId,
                        statusId,
                        java.sql.Date.valueOf(file.businessDay()),
                        file.dataset().name(),
                        file.blobName(),
                        progressiveOf(file),
                        sizeOf(file),
                        file.eTag(),
                        FdrProcessingStatus.PROCESSING.name(),
                        attemptNo,
                        Timestamp.from(now),
                        Timestamp.from(now),
                        Timestamp.from(now)},
                // Types.TIMESTAMP e NON Types.TIMESTAMP_WITH_TIMEZONE, anche se le colonne sono
                // timestamptz: il tipo dichiarato descrive il VALORE bindato, non la colonna, e i
                // valori qui sono java.sql.Timestamp. PgJDBC rifiuta esplicitamente quella
                // combinazione - "Cannot cast an instance of java.sql.Timestamp to type
                // Types.TIMESTAMP_WITH_TIMEZONE" - mentre con Types.TIMESTAMP manda l'istante e
                // lascia convertire al server, che e' il comportamento voluto.
                //
                // NESSUN TEST CON DATABASE FINTO PUO' ACCORGERSENE: un mock accetta qualunque
                // array di tipi. Il difetto e' uscito solo facendo girare l'ingestione contro
                // PostgreSQL vero, e sarebbe esploso alla prima riga della prima giornata.
                new int[] {
                        Types.BIGINT, Types.BIGINT, Types.DATE, Types.VARCHAR, Types.VARCHAR,
                        Types.NUMERIC, Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
                        Types.TIMESTAMP, Types.TIMESTAMP,
                        Types.TIMESTAMP},
                Long.class);
        return id == null ? 0L : id;
    }

    /**
     * Aggiorna i contatori a metà lavoro, lasciando lo stato in lavorazione.
     *
     * <p>Serve a una cosa sola: dopo un crash, sapere <em>dove</em> si era arrivati. Con il solo
     * INSERT iniziale e il solo UPDATE finale, una giornata interrotta mostrerebbe tutti zeri, e
     * "non ha letto niente" e "si è fermato a 2,8 milioni di righe" sarebbero indistinguibili.
     *
     * <p>Il costo è <strong>un UPDATE per batch</strong>, non per riga: su un file da tre milioni di
     * righe con batch da mille sono tremila UPDATE di una riga per chiave primaria, contro tre
     * milioni di INSERT di dato business. Il rapporto è 1 a 1000 e il tipo di scrittura è il più
     * economico che PostgreSQL conosca.
     *
     * <p><strong>Niente {@code COMPLETED_AT}, niente {@code DURATION_MS}, niente stato terminale.</strong>
     * Questa non è una chiusura: una riga che avesse già una data di fine e fosse ancora in
     * lavorazione sarebbe la contraddizione che il log esiste per evitare.
     */
    public void progress(long fileId, FdrFileCounters counters, Instant now) {
        jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_FILE SET ROWS_READ = ?, ROWS_PROCESSED = ?, "
                        + "ROWS_FAILED = ?, ROWS_TRUNCATED = ?, UPDATED_AT = ? WHERE ID = ?",
                counters.rowsRead(), counters.rowsProcessed(), counters.rowsFailed(),
                counters.rowsTruncated(), Timestamp.from(now), fileId);
    }

    /**
     * Chiude la riga con un esito di successo.
     *
     * <p>Va invocata <strong>solo dopo</strong> che la sessione di scrittura è tornata: è la regola
     * non negoziabile del disegno. Nell'ordine inverso una riga potrebbe dichiarare
     * {@code COMPLETED} su dati che non sono mai stati scritti, e quella è una bugia che nessuno
     * scoprirebbe.
     *
     * @param status {@code COMPLETED} oppure {@code COMPLETED_WITH_WARNING}
     */
    public void complete(long fileId, FdrProcessingStatus status, FdrFileCounters counters,
                         Instant now) {
        close(fileId, status, counters, null, null, now);
    }

    /**
     * Chiude la riga con un guasto, <strong>senza perdere i contatori parziali</strong>.
     *
     * <p>Azzerarli sarebbe la cosa peggiore da fare qui: su un file che si è rotto a metà, quanto
     * aveva letto e quanto aveva scritto è esattamente l'informazione per cui si guarda questa
     * riga. E per lo stesso motivo {@code ROWS_SOURCE} resta nullo quando il file non è arrivato a
     * fine lettura — vedi {@link FdrFileCounters}.
     */
    public void fail(long fileId, FdrFileCounters counters, String errorCode, String errorMessage,
                     Instant now) {
        close(fileId, FdrProcessingStatus.FAILED, counters, errorCode,
                clamp(errorMessage, ERROR_MESSAGE_MAX), now);
    }

    /**
     * L'unico UPDATE di chiusura, comune a successo e guasto.
     *
     * <p>{@code DURATION_MS} è calcolata qui dal database: {@code now() - STARTED_AT} letto dalla
     * riga stessa. È il pattern di {@code search_execution_step}, e la ragione è che gli orologi dei
     * pod non coincidono — una durata calcolata in Java fra l'avvio su un pod e la chiusura su un
     * altro potrebbe risultare negativa.
     */
    private void close(long fileId, FdrProcessingStatus status, FdrFileCounters counters,
                       String errorCode, String errorMessage, Instant now) {
        Timestamp at = Timestamp.from(now);
        jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_FILE SET STATUS = ?, ROWS_SOURCE = ?, "
                        + "ROWS_READ = ?, ROWS_PROCESSED = ?, ROWS_FAILED = ?, ROWS_TRUNCATED = ?, "
                        + "ERROR_CODE = ?, ERROR_MESSAGE = ?, COMPLETED_AT = ?, "
                        + "DURATION_MS = CAST(EXTRACT(EPOCH FROM (? - STARTED_AT)) * 1000 AS BIGINT), "
                        + "UPDATED_AT = ? WHERE ID = ?",
                new Object[] {
                        status.name(),
                        counters.rowsSource(),
                        counters.rowsRead(),
                        counters.rowsProcessed(),
                        counters.rowsFailed(),
                        counters.rowsTruncated(),
                        clamp(errorCode, ERROR_CODE_MAX),
                        errorMessage,
                        at,
                        at,
                        at,
                        fileId},
                new int[] {
                        Types.VARCHAR, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.VARCHAR, Types.VARCHAR,
                        Types.TIMESTAMP, Types.TIMESTAMP,
                        Types.TIMESTAMP, Types.BIGINT});
    }

    /**
     * Il progressivo come lo ha letto il parser, senza perdere cifre.
     *
     * <p>La colonna è {@code NUMERIC} e non {@code INTEGER} perché
     * {@code FdrBlobNameParser} usa {@code BigInteger}: il cliente non ha posto un limite al numero
     * di cifre, quindi un file che la discovery accetta deve poter essere registrato qui. Con un
     * intero, il log fallirebbe proprio sul file più anomalo — cioè quello per cui lo si guarda.
     */
    private static BigDecimal progressiveOf(FdrBlobFile file) {
        return file.progressive() == null ? null : new BigDecimal(file.progressive());
    }

    /**
     * La dimensione, o {@code null} quando Azure non la riporta.
     *
     * <p>Azure usa {@code -1} per dire "non lo so", ma scriverlo in colonna lo trasformerebbe in un
     * numero come gli altri: una somma lo sommerebbe, un confronto lo userebbe. Il database ha già
     * {@code NULL} per il dato assente.
     */
    private static Long sizeOf(FdrBlobFile file) {
        return file.sizeBytes() < 0 ? null : file.sizeBytes();
    }

    /** Il database rifiuterebbe un valore più lungo: qui accorciare è legittimo, è testo nostro. */
    private static String clamp(String value, int max) {
        if (value == null || value.length() <= max) {
            return value;
        }
        return value.substring(0, max);
    }
}
