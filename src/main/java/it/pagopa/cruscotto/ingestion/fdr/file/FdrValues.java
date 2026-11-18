package it.pagopa.cruscotto.ingestion.fdr.file;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Turns the text of one CSV field into the type the column holds.
 *
 * <p>Every method here either returns a value or throws {@link FdrValueException} naming the
 * column: there is no silent fallback, no zero for an unparsable number, no epoch for an
 * unparsable date. A row that cannot be mapped is counted and reported, and the file carries on —
 * but it is never written half-understood.
 *
 * <p><strong>Null is a value.</strong> A field the source left empty stays empty: the business
 * columns are all nullable precisely so that an incomplete FDR can still be ingested and seen.
 */
public final class FdrValues {

    /**
     * ISO-8601 with an <em>optional</em> offset.
     *
     * <p>The fractional part is free to be absent or to have up to nine digits — the production
     * files mix 0, 3, 4, 5, 6 and 7 — and this formatter accepts them all.
     *
     * <p>The offset is optional because the columns are {@code TIMESTAMP WITHOUT TIME ZONE}: a
     * value that arrives without one is not an error to reject, it is simply the reading as the
     * source wrote it. Requiring an offset would lose whole rows for a detail the destination
     * cannot store anyway.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ISO_DATE_TIME;

    private FdrValues() {
    }

    // There is no text() here on purpose: a text column needs no conversion at all, and a method
    // that returns its argument would only suggest that something happens to the value.

    /** @throws FdrValueException if the field is not an integer */
    public static Integer integer(String column, String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Integer.valueOf(raw);
        } catch (NumberFormatException e) {
            throw new FdrValueException(column, "not an integer: " + raw);
        }
    }

    /**
     * @throws FdrValueException if the field is not a decimal number
     * @implNote {@link BigDecimal} and never a {@code double}: these are amounts, and the scale the
     *           source wrote is part of the value. A binary floating point type would round
     *           {@code 34.51} on the way in and the database would hold a number the file does not
     *           contain. The decimal separator is the point, as in every observed row; a comma
     *           would be a different convention and is rejected rather than guessed at.
     */
    public static BigDecimal decimal(String column, String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return new BigDecimal(raw);
        } catch (NumberFormatException e) {
            throw new FdrValueException(column, "not a decimal: " + raw);
        }
    }

    /**
     * The timestamp as the source wrote it, with no time-zone arithmetic.
     *
     * <p><strong>The offset is read and discarded, never applied.</strong> The destination columns
     * are {@code TIMESTAMP WITHOUT TIME ZONE}, so what must end up in the database is exactly the
     * reading that is in the file: {@code 2026-09-15T08:38:01.7616300Z} is stored as
     * {@code 2026-09-15 08:38:01.761630}, the same digits. Converting through an instant would be
     * invisible today — every observed value carries {@code Z}, so the conversion is the identity —
     * and would quietly shift a value the day a file arrived with {@code +02:00}, writing an hour
     * that appears nowhere in the source.
     *
     * <p>A {@link LocalDateTime} and not an {@link java.time.Instant} for the same reason: the type
     * must not promise a point on the timeline that the column cannot hold and the data does not
     * always carry.
     *
     * @throws FdrValueException if the field is not an ISO-8601 date-time
     */
    public static LocalDateTime timestamp(String column, String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return TIMESTAMP_FORMAT.parse(raw, LocalDateTime::from);
        } catch (DateTimeParseException e) {
            throw new FdrValueException(column, "not an ISO-8601 date-time: " + raw);
        }
    }
}
