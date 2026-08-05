package it.pagopa.cruscotto.ingestion.massivesearch.report.attempt;

import java.util.List;

/**
 * A single row of {@code tentativi.csv} (one payment attempt / token). Values are already formatted
 * as text and ordered exactly as {@link AttemptReportColumns#HEADERS}. A {@code null} value is
 * rendered as an empty CSV field.
 *
 * @param values ordered column values (same size and order as {@link AttemptReportColumns#HEADERS})
 */
public record AttemptReportRow(List<String> values) {
}
