package it.pagopa.cruscotto.ingestion.massivesearch.csv;

/**
 * A single validation problem detected while parsing a Massive Search CSV.
 *
 * @param lineNumber 1-based line number in the source file ({@code 1} = header)
 * @param column     the offending column name, or {@code null} when the problem is row-level
 * @param message    human-readable description of the problem
 */
public record CsvValidationError(long lineNumber, String column, String message) {

    public static CsvValidationError row(long lineNumber, String message) {
        return new CsvValidationError(lineNumber, null, message);
    }

    public static CsvValidationError column(long lineNumber, String column, String message) {
        return new CsvValidationError(lineNumber, column, message);
    }
}
