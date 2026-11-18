package it.pagopa.cruscotto.ingestion.fdr.persistence;

/**
 * The one authorised truncation in FDR: {@code CAUSALE}, shortened to the width of its column.
 *
 * <p>The customer was shown that 40.570 rows out of 186.857 in a single day — 21,71% — carry a
 * causale longer than 30 characters, and chose to keep the column at 30 and shorten the value
 * rather than widen it. So roughly one row in five loses part of its causale <em>by decision</em>,
 * and finding a shortened causale in the database is this rule at work, not a defect.
 *
 * <p><strong>It exists as a named class for a reason.</strong> A generic clamp applied inline would
 * be indistinguishable from the accident this project must never have: a silent shortening
 * spreading to a second column. Here the field is in the name, the limit is a constant, and the
 * caller has to count what it shortened — see {@link FdrTruncationTally}. Every other column is
 * written whole and an over-long value must fail, visibly.
 *
 * <p><strong>Why not {@code ColumnValueClamp}.</strong> The AS-IS helper is surrogate-safe but
 * counts UTF-16 <em>code units</em>, while PostgreSQL counts {@code varchar(n)} in
 * <em>characters</em>, that is code points. On text outside the Basic Multilingual Plane the two
 * disagree: 30 code units can be as few as 15 code points, so the AS-IS helper would throw away
 * characters that the column would have accepted. Today's data is ASCII and the two coincide, but
 * the contract does not promise ASCII, and a truncation rule is the wrong place to rely on a
 * coincidence.
 */
public final class FdrCausale {

    /** The column width, in Unicode code points, as {@code VARCHAR(30)} counts them. */
    public static final int MAX_LENGTH = 30;

    private FdrCausale() {
    }

    /**
     * The value to persist: the first {@value #MAX_LENGTH} code points, or the value unchanged.
     *
     * <p>Cutting on a code point boundary matters twice over: a lone surrogate would make
     * PostgreSQL reject the INSERT with "invalid byte sequence for encoding UTF8", turning an
     * authorised truncation into the row loss it exists to prevent.
     *
     * @return {@code null} for {@code null}, the same instance when it already fits
     */
    public static String toPersisted(String causale) {
        if (causale == null || causale.length() <= MAX_LENGTH) {
            // Fast path on the UTF-16 length: a string of at most 30 code units cannot hold more
            // than 30 code points, so it fits without counting them.
            return causale;
        }
        int codePoints = causale.codePointCount(0, causale.length());
        if (codePoints <= MAX_LENGTH) {
            return causale;
        }
        return causale.substring(0, causale.offsetByCodePoints(0, MAX_LENGTH));
    }

    /** The source length in code points, which is the number the column compares against. */
    public static int sourceLength(String causale) {
        return causale == null ? 0 : causale.codePointCount(0, causale.length());
    }

    /** Whether persisting this value will lose part of it. */
    public static boolean isTruncated(String causale) {
        return sourceLength(causale) > MAX_LENGTH;
    }
}
