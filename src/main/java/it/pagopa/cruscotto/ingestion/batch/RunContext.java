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
    private long anagraficaLookupCount;
    private long positionLookupCount;
    private long tokenLookupCount;
    private long cacheHitCount;
    private long cacheMissCount;
    private long adxWindowCount;
    private long adxAttemptCount;
    private long emptyWindowCount;
    private long recordsConsolidated;
    private int persistRetryAttempts;
    private long persistRetryWaitMs;
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

    public void incrementAnagraficaLookupCount() {
        this.anagraficaLookupCount++;
    }

    public void incrementPositionLookupCount() {
        this.positionLookupCount++;
    }

    public void incrementTokenLookupCount() {
        this.tokenLookupCount++;
    }

    public void incrementCacheHitCount() {
        this.cacheHitCount++;
    }

    public void incrementCacheMissCount() {
        this.cacheMissCount++;
    }

    public void incrementAdxWindowCount() {
        this.adxWindowCount++;
    }

    public void addAdxAttemptCount(long attempts) {
        this.adxAttemptCount += Math.max(attempts, 0);
    }

    public void incrementEmptyWindowCount() {
        this.emptyWindowCount++;
    }

    public void addRecordsConsolidated(long count) {
        this.recordsConsolidated += Math.max(count, 0);
    }
}
