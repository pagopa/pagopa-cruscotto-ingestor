package it.pagopa.cruscotto.ingestion.massivesearch.report.position;

import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportRow;

import java.util.List;

/**
 * A single row of {@code posizioni.csv}. Values are already formatted as text and ordered exactly as
 * {@link PositionReportColumns#HEADERS}. A {@code null} value is rendered as an empty CSV field.
 *
 * @param values ordered column values (same size and order as {@link PositionReportColumns#HEADERS})
 */
public record PositionReportRow(List<String> values) implements ReportRow {
}
