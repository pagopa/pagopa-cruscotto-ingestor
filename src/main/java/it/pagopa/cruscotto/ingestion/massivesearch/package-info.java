/**
 * Massive Search bounded context (feature 276 - Ricerca Massiva).
 *
 * <p>Self-contained bounded context living inside the {@code cruscotto-ingestor} service but kept
 * separate from the ADX ingestion pipeline. Classes here must not modify or reuse the existing
 * ingestion runners, ADX transformers, checkpoint logic or Quartz ingestion jobs.
 *
 * <p>The feature builds Massive Search instances (by filters or by uploaded CSV), produces a
 * Perimeter CSV of {@code PA,NAV} pairs and three report CSVs ({@code posizioni.csv},
 * {@code tentativi.csv}, {@code versamenti.csv}) compressed into a single ZIP. Only the latest
 * result of each instance is functionally available.
 */
package it.pagopa.cruscotto.ingestion.massivesearch;
