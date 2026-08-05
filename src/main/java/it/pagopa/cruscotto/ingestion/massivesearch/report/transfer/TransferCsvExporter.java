package it.pagopa.cruscotto.ingestion.massivesearch.report.transfer;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * Streaming CSV exporter for {@code versamenti.csv}. Writes the header once and one line per
 * {@link TransferReportRow}, using the configured separator and minimal RFC-4180 quoting.
 */
@Component
public class TransferCsvExporter {

    private static final String NEWLINE = "\r\n";
    private static final char QUOTE = '"';

    private final String separator;

    public TransferCsvExporter(MassiveSearchProperties properties) {
        this.separator = properties.getCsv().getSeparator();
    }

    /** Writes the header row. */
    public void writeHeader(Writer writer) throws IOException {
        writer.write(formatRow(TransferReportColumns.HEADERS));
    }

    /** Writes a single data row. */
    public void writeRow(Writer writer, TransferReportRow row) throws IOException {
        writer.write(formatRow(row.values()));
    }

    private String formatRow(List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(escape(values.get(i)));
        }
        sb.append(NEWLINE);
        return sb.toString();
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
