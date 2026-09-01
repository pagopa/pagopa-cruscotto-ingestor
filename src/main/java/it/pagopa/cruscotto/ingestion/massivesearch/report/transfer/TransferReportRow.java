package it.pagopa.cruscotto.ingestion.massivesearch.report.transfer;

import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportRow;

import java.util.List;

/**
 * A single row of {@code versamenti.csv} (one transfer of a payment attempt). Values are already
 * formatted as text and ordered exactly as {@link TransferReportColumns#HEADERS}. A {@code null}
 * value is rendered as an empty CSV field.
 *
 * @param values ordered column values (same size and order as {@link TransferReportColumns#HEADERS})
 */
public record TransferReportRow(List<String> values) implements ReportRow {
}
