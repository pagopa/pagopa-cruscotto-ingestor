package it.pagopa.cruscotto.ingestion.massivesearch.execution;

/**
 * Raised when a Massive Search execution cannot be completed.
 */
public class MassiveSearchExecutionException extends RuntimeException {

    public MassiveSearchExecutionException(String message) {
        super(message);
    }

    public MassiveSearchExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
