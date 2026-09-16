package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;

public interface AdxClient {

    /**
     * Error returned by {@link #executeQuery} when the run's max-duration budget is already exhausted
     * before the query is even sent. It is NOT a query failure: the caller must treat it as a graceful
     * guardrail stop (COMPLETED / GUARDRAIL_MAX_DURATION), not as an error.
     */
    String MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR = "Max duration guardrail already exceeded before ADX query execution";

    AdxQueryResult executeQuery(RunContext ctx, String database, String query);
}

