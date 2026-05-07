package it.pagopa.cruscotto.ingestion.batch;

/**
 * Generic ingestion runner for executing entity ingestion.
 * TODO: implement the logic for each entity
 */
public interface GenericIngestionRunner {
    void runEntity(RunContext context);
}

