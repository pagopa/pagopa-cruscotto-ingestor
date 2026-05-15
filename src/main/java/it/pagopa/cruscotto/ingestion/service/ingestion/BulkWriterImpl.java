package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;

/**
 * Implementazione bulk writer basata su JdbcTemplate.batchUpdate.
 * Ogni bulk è un'unità atomica (PostgreSQL rollback in caso di errore).
 */
@Slf4j
@Service
public class BulkWriterImpl implements BulkWriter {

    private final JdbcTemplate jdbcTemplate;
    private final String schema;

    public BulkWriterImpl(
            JdbcTemplate jdbcTemplate,
            DbSchemaConfig dbSchemaConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.schema = dbSchemaConfig.getSchemaName();
    }

    @Override
    @Transactional
    public BulkWriteResult writeBulk(EntityName entity, List<?> records, String runId) throws BulkWriteException {
        if (records == null || records.isEmpty()) {
            return new BulkWriteResult(0, Instant.now());
        }

        try {
            int totalRows = switch (entity) {
                case POSITION -> sum(batchUpsertPosition(cast(records, Position.class)));
                case POSITION_TOKENS -> sum(batchUpsertPositionTokens(cast(records, PositionTokens.class)));
                case POSITION_TRANSFERS -> sum(batchUpsertPositionTransfers(cast(records, PositionTransfers.class)));
                case EXTRA_INFO -> sum(batchInsertExtraInfo(cast(records, ExtraInfo.class)));
                case EVENTS_WF -> sum(batchInsertEventsWf(cast(records, EventsWf.class)));
                default -> throw new IllegalArgumentException("No bulk writer configured for entity: " + entity);
            };

            Instant maxTs = Instant.now();
            log.info("[runId={}][entity={}][phase={}] rows={} checkpoint={}",
                    runId, entity, RunPhase.BULK_OK, totalRows, maxTs);
            return new BulkWriteResult(totalRows, maxTs);

        } catch (DataAccessException e) {
            // Errore SQL: nessun record inserito (rollback atomico), NON fare staging, NON aggiornare checkpoint
            log.error("[runId={}][entity={}][phase={}] sqlError={} sqlState={}",
                    runId, entity, RunPhase.BULK_KO_TOTAL, e.getMessage(),
                    e.getMostSpecificCause() != null ? e.getMostSpecificCause().getClass().getSimpleName() : "unknown", e);
            throw new BulkWriteException("Bulk write failed for entity " + entity + ": " + e.getMessage(), e);
        } catch (IllegalArgumentException e) {
            log.error("[runId={}][entity={}][phase={}] error={}",
                    runId, entity, RunPhase.BULK_KO_TOTAL, e.getMessage(), e);
            throw new BulkWriteException(e.getMessage(), e);
        }
    }

    // ---------------------------------------------------------------
    // POSITION
    // ---------------------------------------------------------------
    /**
     * Separate INSERT and UPDATE operations.
     * If Position.id is set, perform UPDATE; otherwise INSERT.
     */
    private int[] batchUpsertPosition(List<Position> records) {
        List<Position> insertsOnly = new java.util.ArrayList<>();
        List<Position> updatesOnly = new java.util.ArrayList<>();

        for (Position p : records) {
            if (p.getId() != null) {
                updatesOnly.add(p);
            } else {
                insertsOnly.add(p);
            }
        }

        int[] result = new int[records.size()];
        int idx = 0;

        // Execute INSERTs
        if (!insertsOnly.isEmpty()) {
            int[] insertResults = batchInsertPosition(insertsOnly);
            System.arraycopy(insertResults, 0, result, idx, insertResults.length);
            idx += insertResults.length;
        }

        // Execute UPDATEs
        if (!updatesOnly.isEmpty()) {
            int[] updateResults = batchUpdatePosition(updatesOnly);
            System.arraycopy(updateResults, 0, result, idx, updateResults.length);
        }

        return result;
    }

    private int[] batchInsertPosition(List<Position> records) {
        String sql = "INSERT INTO " + schema + ".POSITION " +
                "(ID, DATE_EVENT, INSERTED_TIMESTAMP, NAV, PA_EMITTENTE, LAST_EVENT, DATE_EVENTS) " +
                "VALUES (nextval('" + schema + ".SQ_POSITION'), ?, ?, ?, ?, ?, CAST(? AS jsonb))";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Position p = records.get(i);
                ps.setObject(1, p.getDateEvent() != null ? Date.valueOf(p.getDateEvent()) : null);
                ps.setObject(2, p.getInsertedTimestamp() != null ? Timestamp.valueOf(p.getInsertedTimestamp()) : null);
                ps.setString(3, p.getNav());
                ps.setString(4, p.getPaEmittente());
                ps.setObject(5, p.getLastEvent() != null ? Timestamp.valueOf(p.getLastEvent()) : null);
                ps.setString(6, p.getDateEvents() != null ? p.getDateEvents() : "[]");
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }

    private int[] batchUpdatePosition(List<Position> records) {
        String sql = "UPDATE " + schema + ".POSITION " +
                "SET DATE_EVENT = ?, INSERTED_TIMESTAMP = ?, NAV = ?, PA_EMITTENTE = ?, " +
                "LAST_EVENT = ?, DATE_EVENTS = CAST(? AS jsonb) " +
                "WHERE ID = ?";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Position p = records.get(i);
                ps.setObject(1, p.getDateEvent() != null ? Date.valueOf(p.getDateEvent()) : null);
                ps.setObject(2, p.getInsertedTimestamp() != null ? Timestamp.valueOf(p.getInsertedTimestamp()) : null);
                ps.setString(3, p.getNav());
                ps.setString(4, p.getPaEmittente());
                ps.setObject(5, p.getLastEvent() != null ? Timestamp.valueOf(p.getLastEvent()) : null);
                ps.setString(6, p.getDateEvents() != null ? p.getDateEvents() : "[]");
                setNullableInt(ps, 7, p.getId());
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }


    // ---------------------------------------------------------------
    // POSITION_TOKENS
    // ---------------------------------------------------------------
    /**
     * Separate INSERT and UPDATE operations.
     * If PositionTokens.id is set, perform UPDATE; otherwise INSERT.
     */
    private int[] batchUpsertPositionTokens(List<PositionTokens> records) {
        List<PositionTokens> insertsOnly = new java.util.ArrayList<>();
        List<PositionTokens> updatesOnly = new java.util.ArrayList<>();

        for (PositionTokens pt : records) {
            if (pt.getId() != null) {
                updatesOnly.add(pt);
            } else {
                insertsOnly.add(pt);
            }
        }

        int[] result = new int[records.size()];
        int idx = 0;

        // Execute INSERTs
        if (!insertsOnly.isEmpty()) {
            int[] insertResults = batchInsertPositionTokens(insertsOnly);
            System.arraycopy(insertResults, 0, result, idx, insertResults.length);
            idx += insertResults.length;
        }

        // Execute UPDATEs
        if (!updatesOnly.isEmpty()) {
            int[] updateResults = batchUpdatePositionTokens(updatesOnly);
            System.arraycopy(updateResults, 0, result, idx, updateResults.length);
        }

        return result;
    }

    private int[] batchInsertPositionTokens(List<PositionTokens> records) {
        String sql = "INSERT INTO " + schema + ".POSITION_TOKENS " +
                "(ID, DATE_EVENT, FK_POSITION, TOKEN, AMOUNT, FEE, IUV, CREDITOR_REF_ID, " +
                "OUTCOME, ID_CARRELLO, STAZIONE, CANALE, INTERMEDIARIO_PA, INTERMEDIARIO_PSP, " +
                "PSP, TOUCHPOINT, PAYMENT_METHOD, PAYMENT_DATE) " +
                "VALUES (nextval('" + schema + ".SQ_POSITION_TOKENS'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                PositionTokens t = records.get(i);
                ps.setObject(1, t.getDateEvent() != null ? Date.valueOf(t.getDateEvent()) : null);
                setNullableInt(ps, 2, t.getFkPosition());
                ps.setBytes(3, t.getToken());
                ps.setObject(4, t.getAmount(), Types.NUMERIC);
                ps.setObject(5, t.getFee(), Types.NUMERIC);
                ps.setString(6, t.getIuv());
                ps.setString(7, t.getCreditorRefId());
                ps.setString(8, t.getOutcome());
                ps.setString(9, t.getIdCarrello());
                setNullableShort(ps, 10, t.getStazione());
                setNullableShort(ps, 11, t.getCanale());
                setNullableShort(ps, 12, t.getIntermediarioPa());
                setNullableShort(ps, 13, t.getIntermediarioPsp());
                setNullableShort(ps, 14, t.getPsp());
                ps.setString(15, t.getTouchpoint());
                ps.setString(16, t.getPaymentMethod());
                ps.setObject(17, t.getPaymentDate() != null ? Timestamp.valueOf(t.getPaymentDate()) : null);
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }

    private int[] batchUpdatePositionTokens(List<PositionTokens> records) {
        String sql = "UPDATE " + schema + ".POSITION_TOKENS " +
                "SET DATE_EVENT = ?, FK_POSITION = ?, TOKEN = ?, AMOUNT = ?, FEE = ?, " +
                "IUV = ?, CREDITOR_REF_ID = ?, OUTCOME = ?, ID_CARRELLO = ?, STAZIONE = ?, " +
                "CANALE = ?, INTERMEDIARIO_PA = ?, INTERMEDIARIO_PSP = ?, PSP = ?, " +
                "TOUCHPOINT = ?, PAYMENT_METHOD = ?, PAYMENT_DATE = ? " +
                "WHERE ID = ?";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                PositionTokens t = records.get(i);
                ps.setObject(1, t.getDateEvent() != null ? Date.valueOf(t.getDateEvent()) : null);
                setNullableInt(ps, 2, t.getFkPosition());
                ps.setBytes(3, t.getToken());
                ps.setObject(4, t.getAmount(), Types.NUMERIC);
                ps.setObject(5, t.getFee(), Types.NUMERIC);
                ps.setString(6, t.getIuv());
                ps.setString(7, t.getCreditorRefId());
                ps.setString(8, t.getOutcome());
                ps.setString(9, t.getIdCarrello());
                setNullableShort(ps, 10, t.getStazione());
                setNullableShort(ps, 11, t.getCanale());
                setNullableShort(ps, 12, t.getIntermediarioPa());
                setNullableShort(ps, 13, t.getIntermediarioPsp());
                setNullableShort(ps, 14, t.getPsp());
                ps.setString(15, t.getTouchpoint());
                ps.setString(16, t.getPaymentMethod());
                ps.setObject(17, t.getPaymentDate() != null ? Timestamp.valueOf(t.getPaymentDate()) : null);
                setNullableInt(ps, 18, t.getId());
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }

    // ---------------------------------------------------------------
    // POSITION_TRANSFERS
    // ---------------------------------------------------------------
    /**
     * Separate INSERT and UPDATE operations.
     * If PositionTransfers.id is set, perform UPDATE; otherwise INSERT.
     */
    private int[] batchUpsertPositionTransfers(List<PositionTransfers> records) {
        List<PositionTransfers> insertsOnly = new java.util.ArrayList<>();
        List<PositionTransfers> updatesOnly = new java.util.ArrayList<>();

        for (PositionTransfers pt : records) {
            if (pt.getId() != null) {
                updatesOnly.add(pt);
            } else {
                insertsOnly.add(pt);
            }
        }

        int[] result = new int[records.size()];
        int idx = 0;

        // Execute INSERTs
        if (!insertsOnly.isEmpty()) {
            int[] insertResults = batchInsertPositionTransfers(insertsOnly);
            System.arraycopy(insertResults, 0, result, idx, insertResults.length);
            idx += insertResults.length;
        }

        // Execute UPDATEs
        if (!updatesOnly.isEmpty()) {
            int[] updateResults = batchUpdatePositionTransfers(updatesOnly);
            System.arraycopy(updateResults, 0, result, idx, updateResults.length);
        }

        return result;
    }

    private int[] batchInsertPositionTransfers(List<PositionTransfers> records) {
        String sql = "INSERT INTO " + schema + ".POSITION_TRANSFERS " +
                "(ID, DATE_EVENT, FK_TOKEN, PA_TRANSFER, ID_TRANSFER, IBAN_TRANSFER, AMOUNT_TRANSFER, IS_BOLLO) " +
                "VALUES (nextval('" + schema + ".SQ_POSITION_TRANSFERS'), ?, ?, ?, ?, ?, ?, ?)";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                PositionTransfers tr = records.get(i);
                ps.setObject(1, tr.getDateEvent() != null ? Date.valueOf(tr.getDateEvent()) : null);
                setNullableInt(ps, 2, tr.getFkToken());
                ps.setString(3, tr.getPaTransfer());
                setNullableShort(ps, 4, tr.getIdTransfer());
                ps.setString(5, tr.getIbanTransfer());
                ps.setObject(6, tr.getAmountTransfer(), Types.NUMERIC);
                ps.setObject(7, tr.getIsBollo(), Types.BOOLEAN);
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }

    private int[] batchUpdatePositionTransfers(List<PositionTransfers> records) {
        String sql = "UPDATE " + schema + ".POSITION_TRANSFERS " +
                "SET DATE_EVENT = ?, FK_TOKEN = ?, PA_TRANSFER = ?, ID_TRANSFER = ?, " +
                "IBAN_TRANSFER = ?, AMOUNT_TRANSFER = ?, IS_BOLLO = ? " +
                "WHERE ID = ?";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                PositionTransfers tr = records.get(i);
                ps.setObject(1, tr.getDateEvent() != null ? Date.valueOf(tr.getDateEvent()) : null);
                setNullableInt(ps, 2, tr.getFkToken());
                ps.setString(3, tr.getPaTransfer());
                setNullableShort(ps, 4, tr.getIdTransfer());
                ps.setString(5, tr.getIbanTransfer());
                ps.setObject(6, tr.getAmountTransfer(), Types.NUMERIC);
                ps.setObject(7, tr.getIsBollo(), Types.BOOLEAN);
                setNullableInt(ps, 8, tr.getId());
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }

    // ---------------------------------------------------------------
    // EXTRA_INFO
    // ---------------------------------------------------------------
    private int[] batchInsertExtraInfo(List<ExtraInfo> records) {
        String sql = "INSERT INTO " + schema + ".EXTRA_INFO " +
                "(ID, DATE_EVENT, FK_TOKEN, INFO_NAME, INFO_VALUE, TIPO_EVENTO) " +
                "VALUES (nextval('" + schema + ".SQ_EXTRA_INFO'), ?, ?, ?, ?, ?)";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ExtraInfo ei = records.get(i);
                ps.setObject(1, ei.getDateEvent() != null ? Date.valueOf(ei.getDateEvent()) : null);
                setNullableInt(ps, 2, ei.getFkToken());
                ps.setString(3, ei.getInfoName());
                ps.setString(4, ei.getInfoValue());
                setNullableShort(ps, 5, ei.getTipoEvento());
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }

    // ---------------------------------------------------------------
    // EVENTS_WF
    // ---------------------------------------------------------------
    private int[] batchInsertEventsWf(List<EventsWf> records) {
        String sql = "INSERT INTO " + schema + ".EVENTS_WF " +
                "(ID, DATE_EVENT, FK_POSITION, FK_TOKENS, INSERTED_TIMESTAMP_REQ, INSERTED_TIMESTAMP_RESP, " +
                "EVENT_ID_REQ, EVENT_ID_RESP, FAULT_CODE, OUTCOME_REQ, OUTCOME_RESP, TIPO_EVENTO) " +
                "VALUES (nextval('" + schema + ".SQ_EVENTS_WF'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        return jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                EventsWf ev = records.get(i);
                ps.setObject(1, ev.getDateEvent() != null ? Date.valueOf(ev.getDateEvent()) : null);
                setNullableInt(ps, 2, ev.getFkPosition());
                setNullableInt(ps, 3, ev.getFkTokens());
                ps.setObject(4, ev.getInsertedTimestampReq() != null ? Timestamp.valueOf(ev.getInsertedTimestampReq()) : null);
                ps.setObject(5, ev.getInsertedTimestampResp() != null ? Timestamp.valueOf(ev.getInsertedTimestampResp()) : null);
                ps.setString(6, ev.getEventIdReq());
                ps.setString(7, ev.getEventIdResp());
                setNullableShort(ps, 8, ev.getFaultCode());
                ps.setString(9, ev.getOutcomeReq());
                ps.setString(10, ev.getOutcomeResp());
                setNullableShort(ps, 11, ev.getTipoEvento());
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private <T> List<T> cast(List<?> records, Class<T> cls) {
        return (List<T>) records;
    }

    private int sum(int[] counts) {
        int total = 0;
        for (int c : counts) {
            total += (c >= 0 ? c : 1); // Statement.SUCCESS_NO_INFO = -2
        }
        return total;
    }

    private void setNullableInt(PreparedStatement ps, int pos, Integer value) throws SQLException {
        if (value != null) {
            ps.setInt(pos, value);
        } else {
            ps.setNull(pos, Types.INTEGER);
        }
    }

    private void setNullableShort(PreparedStatement ps, int pos, Short value) throws SQLException {
        if (value != null) {
            ps.setShort(pos, value);
        } else {
            ps.setNull(pos, Types.SMALLINT);
        }
    }
}














