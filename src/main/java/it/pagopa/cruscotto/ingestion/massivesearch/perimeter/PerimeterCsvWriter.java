package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Writer;

/**
 * Streaming writer for the Perimeter CSV. Emits the {@code PA,NAV} header and one row per pair,
 * using the configured separator. Values are minimally quoted when they contain the separator,
 * a quote or a line break.
 */
@Component
public class PerimeterCsvWriter {

    private static final String NEWLINE = "\r\n";
    private static final char QUOTE = '"';

    private final String separator;

    public PerimeterCsvWriter(MassiveSearchProperties properties) {
        this.separator = properties.getCsv().getSeparator();
    }

    /** Writes the {@code PA,NAV} header row. */
    public void writeHeader(Writer writer) throws IOException {
        writer.write(formatRow("PA", "NAV"));
    }

    /** Writes a single {@code pa,nav} data row. */
    public void writeRow(Writer writer, String pa, String nav) throws IOException {
        writer.write(formatRow(pa, nav));
    }

    private String formatRow(String pa, String nav) {
        return escape(pa) + separator + escape(nav) + NEWLINE;
    }

    private String escape(String value) {
        if (value == null) {
            return "";
        }
        boolean mustQuote = value.contains(separator)
            || value.indexOf(QUOTE) >= 0
            || value.indexOf('\n') >= 0
            || value.indexOf('\r') >= 0;
        if (!mustQuote) {
            return value;
        }
        return QUOTE + value.replace("\"", "\"\"") + QUOTE;
    }
}
