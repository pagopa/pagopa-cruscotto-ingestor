package it.pagopa.cruscotto.ingestion.massivesearch.execution;

/**
 * Aggregated outcome of the engine pipeline for a single execution.
 */
public record EngineResult(
    String zipPath,
    String zipFileName,
    long zipSizeBytes,
    long totalInputRows,
    long positionRows,
    long attemptRows,
    long transferRows
) {}
