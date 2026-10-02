package it.pagopa.cruscotto.ingestion.massivesearch.report;

import java.util.List;

/**
 * Common contract of a single report row across the three Massive Search reports: an ordered list of
 * already-stringified column values, aligned with the report's {@code *Columns.HEADERS}. It lets the
 * shared {@link AbstractPerimeterReportGenerator} write any report row without knowing its concrete type.
 */
public interface ReportRow {

    /** Ordered column values, same size and order as the report headers. */
    List<String> values();
}
