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
    private BatchLocalCache batchLocalCache = new BatchLocalCache();
}
