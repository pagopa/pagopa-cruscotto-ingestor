package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import java.util.UUID;

/**
 * Minimal projection of a {@code search_instance} row needed by the execution engine.
 */
public record SearchInstanceInfo(UUID id, String name, String inputType, String status) {}
