package it.pagopa.cruscotto.ingestion.massivesearch.facade;

/**
 * Outcome status of a Massive Search execution start requested through {@link MassiveSearchFacade}.
 *
 * <p>Exposed as a stable, technology-neutral vocabulary for the API Layer. Carried as a string in
 * {@link SearchExecutionStartResult#status()} to keep the contract decoupled from this enum.</p>
 */
public enum SearchExecutionStatus {

    /** The execution ran and produced a new latest result. */
    COMPLETED,

    /** The execution was not started because another one is already running for the instance. */
    REJECTED,

    /** The execution started but failed before producing a result. */
    FAILED
}
