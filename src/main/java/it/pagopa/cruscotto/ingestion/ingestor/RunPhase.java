package it.pagopa.cruscotto.ingestion.ingestor;

public enum RunPhase {
    START,
    END,
    WINDOW,
    CHECKPOINT,
    NOOP,
    ERROR,
    SKIP
}

