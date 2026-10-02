package it.pagopa.cruscotto.ingestion.configuration;

/**
 * Run mode of the microservice: which scheduled subsystems are active. The service is launched in a
 * single mode at a time (no mixed Quartz cluster).
 */
public enum AppRunMode {

    /** Only the ADX ingestion jobs run. */
    INGESTOR,

    /** Only the Massive Search scanner runs. */
    MASSIVE_SEARCH,

    /** Both the ingestion jobs and the Massive Search scanner run (historical default). */
    BOTH;

    /** @return {@code true} when the ADX ingestion jobs must run in this mode. */
    public boolean includesIngestor() {
        return this == INGESTOR || this == BOTH;
    }

    /** @return {@code true} when the Massive Search scanner must run in this mode. */
    public boolean includesMassiveSearch() {
        return this == MASSIVE_SEARCH || this == BOTH;
    }
}