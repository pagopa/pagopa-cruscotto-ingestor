package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;

import java.time.Instant;

/**
 * Interface for building entity-specific KustoQL queries for ADX.
 * Each entity (POSITION, POSITION_TOKENS, POSITION_TRANSFERS, EVENTS_WF, EXTRA_INFO)
 * implements this interface to translate client-provided queries.
 */
public interface AdxEntityQueryBuilder {
    /**
     * Build a KustoQL query for reading from ADX.
     *
     * @param ctx              RunContext containing runId, entityName, runStart
     * @param fromInclusive    Start of time window (inclusive)
     * @param toExclusive      End of time window (exclusive)
     * @return                 KustoQL query string ready for execution
     */
    String buildQuery(RunContext ctx, Instant fromInclusive, Instant toExclusive);
}

