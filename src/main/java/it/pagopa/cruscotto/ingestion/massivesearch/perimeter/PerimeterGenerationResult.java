package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

/**
 * Outcome of a perimeter generation request.
 *
 * @param file   metadata of the perimeter CSV (freshly generated or reused)
 * @param reused {@code true} when the CSV already existed and was reused (no regeneration)
 */
public record PerimeterGenerationResult(PerimeterFileMetadata file, boolean reused) {}
