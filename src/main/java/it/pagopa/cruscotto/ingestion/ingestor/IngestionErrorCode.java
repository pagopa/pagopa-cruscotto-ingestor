package it.pagopa.cruscotto.ingestion.ingestor;

public enum IngestionErrorCode {
    MISSING_FOREIGN_KEY,
    TRANSFORMATION_ERROR,
    BULK_WRITE_ERROR,
    VALIDATION_ERROR,
    UNEXPECTED_ERROR
}

