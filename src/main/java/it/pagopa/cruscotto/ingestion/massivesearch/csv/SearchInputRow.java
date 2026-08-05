package it.pagopa.cruscotto.ingestion.massivesearch.csv;

/**
 * Normalized internal representation of a single CSV input row. Fields not carried by the detected
 * template are {@code null}. Values are trimmed; blank values are normalized to {@code null}.
 */
public record SearchInputRow(String nav, String pa, String iuv, String token) {}
