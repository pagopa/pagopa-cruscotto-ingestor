package it.pagopa.cruscotto.ingestion.massivesearch.report.token;

import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportRow;

import java.util.List;

/**
 * A single row of {@code tentativi.csv} (one payment token). Values are already formatted
 * as text and ordered exactly as {@link TokenReportColumns#HEADERS}. A {@code null} value is
 * rendered as an empty CSV field.
 *
 * @param values ordered column values (same size and order as {@link TokenReportColumns#HEADERS})
 */
public record TokenReportRow(List<String> values) implements ReportRow {
}
