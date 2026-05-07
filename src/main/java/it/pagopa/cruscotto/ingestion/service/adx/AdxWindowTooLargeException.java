package it.pagopa.cruscotto.ingestion.service.adx;

import java.time.Duration;
import java.time.Instant;

public class AdxWindowTooLargeException extends RuntimeException {
    private final String runId;
    private final String entityName;
    private final Instant cursor;
    private final Duration window;

    public AdxWindowTooLargeException(String runId, String entityName, Instant cursor, Duration window) {
        super("ADX window too large: runId=" + runId + ", entityName=" + entityName +
              ", cursor=" + cursor + ", window=" + window);
        this.runId = runId;
        this.entityName = entityName;
        this.cursor = cursor;
        this.window = window;
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
}

