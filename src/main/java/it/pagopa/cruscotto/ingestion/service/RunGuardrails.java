package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;

public interface RunGuardrails {
    boolean ok(RunContext ctx, long queriesExecuted, long rowsProcessed);
}
