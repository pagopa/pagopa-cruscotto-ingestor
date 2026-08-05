package it.pagopa.cruscotto.ingestion.massivesearch.execution;

/**
 * Phases of the Massive Search execution pipeline, tracked in {@code search_execution_step}.
 */
public enum StepPhase {
    PERIMETER,
    ANALYSIS_WINDOW,
    POSITION,
    ATTEMPT,
    TRANSFER,
    ZIP;

    /** Maps a report type to its corresponding pipeline phase. */
    static StepPhase fromReportType(ReportType type) {
        return switch (type) {
            case POSITION -> POSITION;
            case ATTEMPT -> ATTEMPT;
            case TRANSFER -> TRANSFER;
        };
    }
}
