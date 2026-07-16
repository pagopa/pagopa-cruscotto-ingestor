package it.pagopa.cruscotto.ingestion.batch;

import it.pagopa.cruscotto.ingestion.service.ingestion.BatchLocalCache;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.time.Instant;

@Data
@RequiredArgsConstructor
public class RunContext {
    private final String entityName;
    private final String runId;
    private final Instant runStart;
    private String operationId;
    private boolean catchupMode;
    private long adxQueryDurationMs;
    private long ingestorLogicDurationMs;
    private long postgresInsertDurationMs;
    private long anagraficaDurationMs;
    private long fkPositionDurationMs;
    private long fkTokenDurationMs;
    private BatchLocalCache batchLocalCache = new BatchLocalCache();

    public void addAnagraficaDurationMs(long durationMs) {
        this.anagraficaDurationMs += Math.max(durationMs, 0);
    }

    public void addFkPositionDurationMs(long durationMs) {
        this.fkPositionDurationMs += Math.max(durationMs, 0);
    }

    public void addFkTokenDurationMs(long durationMs) {
        this.fkTokenDurationMs += Math.max(durationMs, 0);
    }
}
