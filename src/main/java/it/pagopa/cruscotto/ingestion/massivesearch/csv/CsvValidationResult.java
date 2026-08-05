package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import java.util.List;

/**
 * Result of validating a Massive Search CSV. Row counts refer to data rows (header excluded).
 *
 * @param valid           {@code true} when the template is recognized and no problems were found
 * @param detectedTemplate the detected template ({@link CsvTemplate#UNKNOWN} when unrecognized)
 * @param totalRows       number of data rows read
 * @param validRows       number of data rows that passed validation
 * @param invalidRows     number of data rows that failed validation
 * @param errors          collected validation errors (may be capped, see configuration)
 */
public record CsvValidationResult(
    boolean valid,
    CsvTemplate detectedTemplate,
    long totalRows,
    long validRows,
    long invalidRows,
    List<CsvValidationError> errors
) {}
