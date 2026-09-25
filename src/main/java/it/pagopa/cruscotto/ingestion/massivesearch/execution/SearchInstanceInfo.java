package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import java.util.Set;
import java.util.UUID;

/**
 * Minimal projection of a {@code search_instance} row needed by the execution engine.
 *
 * @param reports the report types selected for this instance (never empty; defaults to all)
 */
public record SearchInstanceInfo(UUID id, String name, String inputType, String status, Set<ReportType> reports) {}
