package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import java.io.IOException;
import java.io.Writer;

/**
 * SPI implemented by each report module (position / attempt / transfer). The engine owns the
 * physical storage of the report file and provides a {@link Writer}; the implementation streams the
 * report rows into it and returns the number of data rows written.
 *
 * <p>Implementations are contributed by the report-generation steps (packages
 * {@code massivesearch.report.*}). When a generator for a given {@link ReportType} is absent, the
 * engine writes an empty placeholder report.</p>
 */
public interface SearchReportGenerator {

    /** The report type this generator produces. */
    ReportType type();

    /**
     * Streams the report content for the given execution.
     *
     * @param context the current execution context
     * @param writer  destination writer (managed and closed by the engine)
     * @return the number of data rows written
     */
    long writeReport(MassiveSearchExecutionContext context, Writer writer) throws IOException;
}
