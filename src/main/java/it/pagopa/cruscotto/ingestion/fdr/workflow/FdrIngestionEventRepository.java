package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;

/**
 * L'audit della giornata: un evento per fatto, mai uno per riga.
 *
 * <p>La tabella serve a <em>leggere</em> una giornata — perché è finita in warning, quante causali
 * sono state accorciate, quali file erano vuoti — non a conservarne una copia. Il dettaglio della
 * singola riga scartata vive in {@code STG_INGEST_ERROR}, che è la sua sede, e qui non va
 * duplicato.
 *
 * <p>{@code DETAIL} è {@code VARCHAR(4000)} e {@code MESSAGE} {@code VARCHAR(1000)}: entrambi
 * vengono accorciati prima della scrittura. Un evento che fa fallire il proprio INSERT per un
 * messaggio lungo perderebbe esattamente la diagnosi che stava registrando.
 */
@Repository
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrIngestionEventRepository {

    static final int MAX_MESSAGE = 1000;
    static final int MAX_DETAIL = 4000;

    private final JdbcTemplate jdbcTemplate;
    private final String schema;

    public FdrIngestionEventRepository(JdbcTemplate jdbcTemplate, DbSchemaConfig dbSchemaConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    /**
     * @param dataset  {@code null} per un evento di giornata che non appartiene a un dataset
     * @param blobName {@code null} quando l'evento non riguarda un file preciso
     */
    public void record(long batchId, Long statusId, LocalDate businessDay, FdrDataset dataset,
                       String blobName, String eventType, String severity, String eventCode,
                       String message, String detail, Instant now) {
        jdbcTemplate.update("INSERT INTO " + schema + ".FDR_INGESTION_EVENT "
                        + "(ID, BATCH_ID, STATUS_ID, BUSINESS_DAY, DATASET, BLOB_NAME, EVENT_TYPE, "
                        + "SEVERITY, EVENT_CODE, MESSAGE, DETAIL, CREATED_AT) "
                        + "VALUES (nextval('" + schema + ".SQ_FDR_INGESTION_EVENT'), "
                        + "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                new Object[] {batchId, statusId, java.sql.Date.valueOf(businessDay),
                        dataset == null ? null : dataset.name(), blobName, eventType, severity,
                        eventCode, shorten(message, MAX_MESSAGE), shorten(detail, MAX_DETAIL),
                        Timestamp.from(now)},
                new int[] {Types.BIGINT, Types.BIGINT, Types.DATE, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.TIMESTAMP_WITH_TIMEZONE});
    }

    static String shorten(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        String flat = value.replace('\n', ' ').replace('\r', ' ').trim();
        return flat.length() <= maxLength ? flat : flat.substring(0, maxLength);
    }
}
