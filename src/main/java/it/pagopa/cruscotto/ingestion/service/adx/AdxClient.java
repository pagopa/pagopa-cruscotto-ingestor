package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;

public interface AdxClient {
    AdxQueryResult executeQuery(RunContext ctx, String database, String query);
}

