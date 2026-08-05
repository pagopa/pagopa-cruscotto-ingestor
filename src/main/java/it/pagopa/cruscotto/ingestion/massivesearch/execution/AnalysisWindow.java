package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import java.time.LocalDateTime;

/**
 * Optional temporal window that limits the analysis of an execution to the payments whose
 * {@code position_tokens.payment_date} falls in {@code [fromInclusive, toExclusive)}.
 *
 * <p>Both bounds are optional: a {@code null} bound means "no restriction on that side". When both
 * are {@code null} the window is a no-op (the full history is analysed). Applies to both FILTER and
 * CSV instances; for CSV searches it corresponds to the optional period the operator may provide.</p>
 *
 * @param fromInclusive inclusive lower bound (start of day), or {@code null}
 * @param toExclusive   exclusive upper bound (start of the day after the selected end), or {@code null}
 */
public record AnalysisWindow(LocalDateTime fromInclusive, LocalDateTime toExclusive) {

    private static final AnalysisWindow NONE = new AnalysisWindow(null, null);

    /** @return the no-op window (analyse the full history). */
    public static AnalysisWindow none() {
        return NONE;
    }

    /** @return {@code true} when at least one bound restricts the analysis. */
    public boolean hasBounds() {
        return fromInclusive != null || toExclusive != null;
    }
}
