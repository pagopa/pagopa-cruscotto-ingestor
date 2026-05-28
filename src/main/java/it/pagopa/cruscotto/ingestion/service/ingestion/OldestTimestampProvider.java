package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;

import java.time.Instant;
import java.util.Optional;

public interface OldestTimestampProvider {
    Optional<Instant> getOldestTimestamp(RunContext ctx, EntityName entity);
}

