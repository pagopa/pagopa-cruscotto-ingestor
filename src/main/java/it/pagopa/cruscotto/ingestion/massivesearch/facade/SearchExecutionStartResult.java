package it.pagopa.cruscotto.ingestion.massivesearch.facade;

import java.util.UUID;

/**
 * Outcome returned when a Massive Search execution is started through {@link MassiveSearchFacade}.
 *
 * @param instanceId  identifier of the Massive Search instance
 * @param executionId identifier of the started execution, or {@code null} when no execution was
 *                    created (e.g. the request was rejected because one is already running)
 * @param status      execution outcome, one of {@link SearchExecutionStatus} names
 */
public record SearchExecutionStartResult(UUID instanceId, UUID executionId, String status) {

    public static SearchExecutionStartResult completed(UUID instanceId, UUID executionId) {
        return new SearchExecutionStartResult(instanceId, executionId, SearchExecutionStatus.COMPLETED.name());
    }

    public static SearchExecutionStartResult rejected(UUID instanceId) {
        return new SearchExecutionStartResult(instanceId, null, SearchExecutionStatus.REJECTED.name());
    }

    public static SearchExecutionStartResult failed(UUID instanceId, UUID executionId) {
        return new SearchExecutionStartResult(instanceId, executionId, SearchExecutionStatus.FAILED.name());
    }
}
