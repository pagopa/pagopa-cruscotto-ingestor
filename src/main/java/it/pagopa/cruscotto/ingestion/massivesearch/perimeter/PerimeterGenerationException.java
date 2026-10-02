package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

/**
 * Raised when the perimeter CSV cannot be generated for a search instance.
 */
public class PerimeterGenerationException extends RuntimeException {

    public PerimeterGenerationException(String message, Throwable cause) {
        super(message, cause);
    }

    public PerimeterGenerationException(String message) {
        super(message);
    }
}
