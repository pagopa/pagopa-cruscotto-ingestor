package it.pagopa.cruscotto.ingestion.massivesearch.execution;

/**
 * Lifecycle states of a Massive Search execution (mirrors {@code search_execution.status}).
 */
public enum ExecutionStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
}
