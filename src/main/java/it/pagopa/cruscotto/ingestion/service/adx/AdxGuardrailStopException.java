package it.pagopa.cruscotto.ingestion.service.adx;

import java.time.Instant;

/**
 * Raised by {@code AdxQueryService.fetchWindow} when the ADX client refuses a query because the run's
 * max-duration budget is already exhausted ({@link AdxClient#MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR}).
 *
 * <p>This is NOT a failure: it is the run hitting its (catch-up aware) time budget. The runner must
 * treat it as a graceful guardrail stop — end reason {@code GUARDRAIL_MAX_DURATION}, status
 * {@code COMPLETED} — exactly like the in-loop guardrail check, rather than marking the run FAILED.
 * Distinct from {@link AdxQueryFailedException} (real, actionable ADX errors) and
 * {@link AdxWindowTooLargeException} (result-set too large, handled by halving).</p>
 */
public class AdxGuardrailStopException extends RuntimeException {

    private final String runId;
    private final String entityName;
    private final Instant cursor;

    public AdxGuardrailStopException(String runId, String entityName, Instant cursor) {
        super("ADX query skipped: max-duration guardrail exhausted mid-run: runId=" + runId
                + ", entityName=" + entityName + ", cursor=" + cursor);
        this.runId = runId;
        this.entityName = entityName;
        this.cursor = cursor;
    }

    public String getRunId() {
        return runId;
    }

    public String getEntityName() {
        return entityName;
    }

    public Instant getCursor() {
        return cursor;
    }
}
