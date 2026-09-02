package it.pagopa.cruscotto.ingestion.config;

import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Configuration for ADX (Kusto) table names.
 * Allows mapping entity names to table names in external configuration.
 *
 * Usage:
 * ingestion.adx.tables:
 *   POSITION: SERT_POSITION
 *   POSITION_TOKENS: SERT_POSITION_TOKENS
 *   ...
 */
@Component
@ConfigurationProperties(prefix = "ingestion.adx")
@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdxTableNamesConfig {
    private Map<String, String> tables = new HashMap<>();

    public void setTables(Map<String, String> tables) {
        this.tables = new HashMap<>();
        if (tables == null) {
            return;
        }
        tables.forEach((key, value) -> this.tables.put(normalizeKey(key), value));
    }

    public String getTableName(String entityName) {
        String tableName = tables.get(normalizeKey(entityName));
        if (tableName == null) {
            throw new IllegalArgumentException("ADX table name not configured for entity: " + entityName);
        }
        log.debug("ADX_TABLE_NAME_RESOLVED entity={} table={}", normalizeKey(entityName), tableName);
        return tableName;
    }

    @PostConstruct
    public void logConfiguredTables() {
        log.info("ADX_TABLES_CONFIGURED {}", tables);
    }

    private String normalizeKey(String key) {
        return key
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .replaceAll("[^A-Za-z0-9]+", "_")
                .replaceAll("_+", "_")
                .replaceAll("(^_)|(_$)", "")
                .toUpperCase(Locale.ROOT);
    }
}


