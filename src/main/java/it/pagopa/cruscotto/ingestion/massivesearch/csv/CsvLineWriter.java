package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * Shared streaming CSV line writer: formats a list of already-stringified values into a single CSV
 * record (configured separator, CRLF terminator) applying minimal RFC-4180 quoting. Stateless and
 * reused by every report so the escaping rules live in exactly one place.
 */
@Component
public class CsvLineWriter {

    private static final String NEWLINE = "\r\n";
    private static final char QUOTE = '"';

    private final String separator;

    public CsvLineWriter(MassiveSearchProperties properties) {
        this.separator = properties.getCsv().getSeparator();
    }

    /** Writes a single CSV record: values joined by the separator and terminated by CRLF. */
    public void writeLine(Writer writer, List<String> values) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(escape(values.get(i)));
        }
        sb.append(NEWLINE);
        writer.write(sb.toString());
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
