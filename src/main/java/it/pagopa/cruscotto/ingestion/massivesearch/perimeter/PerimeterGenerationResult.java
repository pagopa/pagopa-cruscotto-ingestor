package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import it.pagopa.cruscotto.ingestion.massivesearch.execution.StepMetrics;

/**
 * Outcome of a perimeter generation request.
 *
 * @param file    metadata of the perimeter CSV (freshly generated or reused)
 * @param reused  {@code true} when the CSV already existed and was reused (no regeneration)
 * @param metrics diagnostica della fase, persistita sulla riga di step del perimetro: viene chiusa
 *                prima che i report partano, quindi resta disponibile anche se un report va in timeout
 */
public record PerimeterGenerationResult(PerimeterFileMetadata file, boolean reused, StepMetrics metrics) {}
