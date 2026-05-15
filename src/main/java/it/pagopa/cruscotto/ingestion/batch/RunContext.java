package it.pagopa.cruscotto.ingestion.batch;

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
}
