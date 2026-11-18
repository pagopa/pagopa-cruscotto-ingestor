package it.pagopa.cruscotto.ingestion.fdr.file;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
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
     * ISO-8601 with an explicit offset, which every observed timestamp carries as {@code Z}.
     *
     * <p>The fractional part is free to be absent or to have up to nine digits — the production
     * files mix 0, 3, 4, 5, 6 and 7 — and this formatter accepts them all.
     *
     * <p>An offset is <em>required</em>, deliberately. A timestamp without one is not a timestamp
     * that defaults to UTC, it is a timestamp whose zone nobody knows, and guessing would put a
     * settlement an hour or two away from where it happened. The database columns are
     * {@code TIMESTAMP} without zone, so the conversion to UTC happens once, downstream, on a value
     * that is unambiguous here.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

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

    /** @throws FdrValueException if the field is not an ISO-8601 timestamp with an offset */
    public static Instant timestamp(String column, String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(raw, TIMESTAMP_FORMAT).toInstant();
        } catch (DateTimeParseException e) {
            throw new FdrValueException(column, "not an ISO-8601 timestamp with offset: " + raw);
        }
    }
}
