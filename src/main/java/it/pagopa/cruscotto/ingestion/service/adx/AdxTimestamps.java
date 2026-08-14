package it.pagopa.cruscotto.ingestion.service.adx;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Locale;
import java.util.Optional;

/**
 * Robust conversion of an ADX result cell into an {@link Instant}.
 *
 * <p>ADX may return a datetime column already typed (Instant/OffsetDateTime/…) or, depending on the
 * driver/serialization, as a String in various formats. This centralizes the tolerant parsing so both
 * the oldest-timestamp bootstrap and the empty-window probe interpret timestamps identically.</p>
 *
 * @return the parsed instant, or empty when the value is {@code null} or cannot be parsed as a timestamp.
 */
public final class AdxTimestamps {

    private static final DateTimeFormatter ITALIAN_DATETIME =
            DateTimeFormatter.ofPattern("dd/MM/yyyy, HH:mm:ss,SSSSSS", Locale.ITALY);

    private AdxTimestamps() {
    }

    public static Optional<Instant> toInstant(Object value) {
        if (value == null) {
            return Optional.empty();
        }
        if (value instanceof Instant instant) {
            return Optional.of(instant);
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return Optional.of(offsetDateTime.toInstant());
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return Optional.of(zonedDateTime.toInstant());
        }
        if (value instanceof LocalDateTime localDateTime) {
            return Optional.of(localDateTime.toInstant(ZoneOffset.UTC));
        }
        if (value instanceof Timestamp timestamp) {
            return Optional.of(timestamp.toInstant());
        }
        if (value instanceof Date date) {
            return Optional.of(date.toInstant());
        }
        if (value instanceof Number number) {
            return Optional.of(Instant.ofEpochMilli(number.longValue()));
        }
        if (value instanceof CharSequence charSequence) {
            String timestamp = charSequence.toString().trim();
            if (timestamp.isEmpty()) {
                return Optional.empty();
            }
            try {
                return Optional.of(Instant.parse(timestamp));
            } catch (DateTimeParseException ignored) {
                try {
                    return Optional.of(OffsetDateTime.parse(timestamp).toInstant());
                } catch (DateTimeParseException ignoredAgain) {
                    try {
                        return Optional.of(LocalDateTime.parse(timestamp).toInstant(ZoneOffset.UTC));
                    } catch (DateTimeParseException ignoredThird) {
                        try {
                            return Optional.of(LocalDateTime.parse(timestamp, ITALIAN_DATETIME).toInstant(ZoneOffset.UTC));
                        } catch (DateTimeParseException ignoredFourth) {
                            return Optional.empty();
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }
}
