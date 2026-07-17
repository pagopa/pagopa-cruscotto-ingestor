package it.pagopa.cruscotto.ingestion.repository;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * Lettura del registry canonico TOKEN -> prima DATE_EVENT.
 * Il registry è popolato in "first-write-wins" (BulkWriterImpl); qui viene solo letto
 * per ricavare la partizione (FIRST_DATE_EVENT) della riga canonica in POSITION_TOKENS,
 * abilitando il partition pruning nelle lookup per TOKEN. Accesso via JdbcTemplate per
 * coerenza con le altre operazioni sul registry.
 */
@Component
@RequiredArgsConstructor
public class PositionTokenRegistryReader {

    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;

    public Optional<LocalDate> findFirstDateEventByToken(byte[] token) {
        if (token == null) {
            return Optional.empty();
        }
        String sql = "SELECT FIRST_DATE_EVENT FROM " + dbSchemaConfig.getSchemaName()
                + ".POSITION_TOKEN_REGISTRY WHERE TOKEN = ?";
        List<Date> rows = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getDate(1), token);
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(rows.get(0)).map(Date::toLocalDate);
    }
}
