package it.pagopa.cruscotto.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration for database schema name.
 * Allows schema name to be configured externally.
 *
 * Usage:
 * ingestion.database.schema: sert_ingestor
 *
 * Or via environment variable:
 * INGESTION_DATABASE_SCHEMA=sert_ingestor
 */
@Component
@ConfigurationProperties(prefix = "ingestion.database")
public class DbSchemaConfig {
    private String schema = "sert_ingestor";

    public String getSchemaName() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}


