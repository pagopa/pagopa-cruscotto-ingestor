package it.pagopa.cruscotto.ingestion.service.adx;

import java.time.Duration;
import java.time.Instant;

/**
 * Raised when an ADX window query fails with an error that halving the window cannot fix (so it is
 * not an {@link AdxWindowTooLargeException}).
 *
 * <p>It carries the vendor error message so the run ends as {@code FAILED} with
 * {@code ERROR_CODE}/{@code ERROR_MESSAGE} filled in {@code INGEST_EXECUTION_LOG}: the query failure
 * must not be reported as a healthy {@code COMPLETED} run, because no window was read and the
 * checkpoint did not advance — on a permanent error the entity would stop progressing silently.</p>
 */
public class AdxQueryFailedException extends RuntimeException {

    private final String runId;
    private final String entityName;
    private final Instant cursor;
    private final Duration window;
    private final String adxError;

    public AdxQueryFailedException(String runId, String entityName, Instant cursor, Duration window, String adxError) {
        super("ADX query failed: runId=" + runId + ", entityName=" + entityName
                + ", cursor=" + cursor + ", window=" + window + ", adxError=" + adxError);
        this.runId = runId;
        this.entityName = entityName;
        this.cursor = cursor;
        this.window = window;
        this.adxError = adxError;
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

    public Duration getWindow() {
        return window;
    }

    /** The error message returned by ADX, persisted in the execution log for diagnosis. */
    public String getAdxError() {
        return adxError;
    }
}
