package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import java.util.UUID;

/**
 * Lightweight reference to a {@code search_execution} row stuck in {@code RUNNING}, used by the
 * stuck-execution recovery to fail both the execution and its owning instance.
 *
 * @param executionId identifier of the stuck execution
 * @param instanceId  identifier of the owning search instance
 */
public record StuckExecutionRef(UUID executionId, UUID instanceId) {
}
