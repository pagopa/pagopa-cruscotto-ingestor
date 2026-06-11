package it.pagopa.cruscotto.ingestion.config;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AdxTableNamesConfigTest {

    @Test
    void shouldResolveUpperSnakeCaseEntityNames() {
        AdxTableNamesConfig config = new AdxTableNamesConfig();
        config.setTables(Map.of(
                "POSITION", "SERT_POSITION",
                "POSITION_TRANSFERS", "SERT_TRANSFERS"
        ));

        assertEquals("SERT_TRANSFERS", config.getTableName("POSITION_TRANSFERS"));
    }

    @Test
    void shouldResolveKeysAcrossCommonNamingVariants() {
        AdxTableNamesConfig config = new AdxTableNamesConfig();
        config.setTables(Map.of("position-transfers", "SERT_TRANSFERS"));

        assertEquals("SERT_TRANSFERS", config.getTableName("POSITION_TRANSFERS"));
        assertEquals("SERT_TRANSFERS", config.getTableName("positionTransfers"));
    }

    @Test
    void shouldFailFastWhenEntityIsMissing() {
        AdxTableNamesConfig config = new AdxTableNamesConfig();
        config.setTables(Map.of("POSITION", "SERT_POSITION"));

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.getTableName("POSITION_TRANSFERS")
        );

        assertEquals("ADX table name not configured for entity: POSITION_TRANSFERS", exception.getMessage());
    }
}

