package it.pagopa.cruscotto.ingestion.massivesearch.execution;

/**
 * Descriptor of a generated report file.
 *
 * @param type        the report type
 * @param storagePath the resolved storage path of the file
 * @param fileName    the report file name
 * @param rows        number of data rows written
 */
public record ReportOutput(ReportType type, String storagePath, String fileName, long rows) {}
