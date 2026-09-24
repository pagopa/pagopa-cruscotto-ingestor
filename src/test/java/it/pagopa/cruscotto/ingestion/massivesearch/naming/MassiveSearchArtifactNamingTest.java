package it.pagopa.cruscotto.ingestion.massivesearch.naming;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the result ZIP and the report CSVs it contains share the same executionId short id
 * and the same timestamp token, so an archive and its inner files are visibly a single set.
 */
class MassiveSearchArtifactNamingTest {

    private final MassiveSearchProperties properties = new MassiveSearchProperties();
    private final MassiveSearchArtifactNaming naming = new MassiveSearchArtifactNaming(properties);

    private final UUID executionId = UUID.fromString("a1b2c3d4-0000-0000-0000-000000000000");
    private final LocalDateTime timestamp = LocalDateTime.of(2026, 8, 4, 15, 35, 0);

    @Test
    void reportCsvsFollowTheSameConventionAsTheZip() {
        String zip = naming.resultZipFileName(executionId, timestamp);
        String position = naming.reportFileName(properties.getReports().getPositionPrefix(), executionId, timestamp);
        String attempt = naming.reportFileName(properties.getReports().getTokenPrefix(), executionId, timestamp);
        String transfer = naming.reportFileName(properties.getReports().getTransferPrefix(), executionId, timestamp);

        assertEquals("ricerca-massiva__a1b2c3d4__20260804-153500.zip", zip);
        assertEquals("posizioni__a1b2c3d4__20260804-153500.csv", position);
        assertEquals("tentativi__a1b2c3d4__20260804-153500.csv", attempt);
        assertEquals("versamenti__a1b2c3d4__20260804-153500.csv", transfer);
    }

    @Test
    void reportAndZipShareShortIdAndTimestamp() {
        String zip = naming.resultZipFileName(executionId, timestamp);
        String position = naming.reportFileName(properties.getReports().getPositionPrefix(), executionId, timestamp);

        String sharedSuffix = "a1b2c3d4__20260804-153500";
        assertTrue(zip.contains(sharedSuffix), zip);
        assertTrue(position.contains(sharedSuffix), position);
    }

    @Test
    void perimeterFileNameFollowsTheConventionWithItsOwnTimestamp() {
        UUID instanceId = UUID.fromString("deadbeef-0000-0000-0000-000000000000");

        String perimeter = naming.perimeterFileName(instanceId);

        // perimetro__<8hex-instanceId>__<yyyyMMdd-HHmmss>.csv
        assertTrue(perimeter.matches("perimetro__deadbeef__\\d{8}-\\d{6}\\.csv"), perimeter);
    }

    @Test
    void shortIdIsClampedToTheAvailableHexLength() {
        properties.getNaming().setShortIdLength(64); // longer than the 32 hex chars of a UUID
        UUID id = UUID.fromString("a1b2c3d4-e5f6-7890-abcd-ef0123456789");

        String zip = naming.resultZipFileName(id, timestamp);

        // Uses the full 32-char hex (no dashes) rather than overflowing.
        assertTrue(zip.startsWith("ricerca-massiva__a1b2c3d4e5f67890abcdef0123456789__"), zip);
    }
}