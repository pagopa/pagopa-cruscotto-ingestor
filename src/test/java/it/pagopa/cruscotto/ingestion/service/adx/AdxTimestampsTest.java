package it.pagopa.cruscotto.ingestion.service.adx;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the tolerant ADX timestamp parsing shared by the oldest-timestamp bootstrap and the
 * empty-window probe. ADX may return a datetime already typed or as a String in several formats;
 * a {@code null} or unparseable value must yield empty (never a wrong instant).
 */
class AdxTimestampsTest {

    private static final Instant EXPECTED = Instant.parse("2026-08-03T00:00:00Z");

    @Test
    void nullYieldsEmpty() {
        assertTrue(AdxTimestamps.toInstant(null).isEmpty());
    }

    @Test
    void passesThroughInstant() {
        assertEquals(Optional.of(EXPECTED), AdxTimestamps.toInstant(EXPECTED));
    }

    @Test
    void convertsOffsetDateTime() {
        OffsetDateTime odt = OffsetDateTime.parse("2026-08-03T02:00:00+02:00");
        assertEquals(Optional.of(EXPECTED), AdxTimestamps.toInstant(odt));
    }

    @Test
    void convertsZonedDateTime() {
        ZonedDateTime zdt = OffsetDateTime.parse("2026-08-03T02:00:00+02:00").toZonedDateTime();
        assertEquals(Optional.of(EXPECTED), AdxTimestamps.toInstant(zdt));
    }

    @Test
    void treatsLocalDateTimeAsUtc() {
        LocalDateTime ldt = LocalDateTime.parse("2026-08-03T00:00:00");
        assertEquals(Optional.of(EXPECTED), AdxTimestamps.toInstant(ldt));
    }

    @Test
    void convertsSqlTimestamp() {
        Timestamp ts = Timestamp.from(EXPECTED);
        assertEquals(Optional.of(EXPECTED), AdxTimestamps.toInstant(ts));
    }

    @Test
    void convertsEpochMillisNumber() {
        assertEquals(Optional.of(Instant.ofEpochMilli(1_000L)), AdxTimestamps.toInstant(1_000L));
    }

    @Test
    void parsesIsoStringWithZ() {
        assertEquals(Optional.of(EXPECTED), AdxTimestamps.toInstant("2026-08-03T00:00:00Z"));
    }

    @Test
    void parsesOffsetString() {
        assertEquals(Optional.of(EXPECTED), AdxTimestamps.toInstant("2026-08-03T02:00:00+02:00"));
    }

    @Test
    void parsesLocalDateTimeString() {
        assertEquals(Optional.of(EXPECTED), AdxTimestamps.toInstant("2026-08-03T00:00:00"));
    }

    @Test
    void parsesItalianFormattedString() {
        assertEquals(Optional.of(Instant.parse("2026-08-03T00:00:00.123009Z")),
                AdxTimestamps.toInstant("03/08/2026, 00:00:00,123009"));
    }

    @Test
    void blankOrGarbageStringYieldsEmpty() {
        assertTrue(AdxTimestamps.toInstant("").isEmpty());
        assertTrue(AdxTimestamps.toInstant("   ").isEmpty());
        assertTrue(AdxTimestamps.toInstant("not-a-date").isEmpty());
    }

    @Test
    void unsupportedTypeYieldsEmpty() {
        assertTrue(AdxTimestamps.toInstant(new Object()).isEmpty());
    }
}
