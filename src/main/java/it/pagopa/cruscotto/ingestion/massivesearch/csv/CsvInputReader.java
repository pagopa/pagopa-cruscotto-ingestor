package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplateDetector.TemplateDetection;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Low-level streaming CSV utilities: opens a reader with the configured charset, parses a single
 * line into fields (RFC-4180 style quoting) and normalizes a parsed line into a {@link SearchInputRow}.
 *
 * <p>No method loads the whole file in memory; callers iterate the source reader line by line.</p>
 */
@Component
public class CsvInputReader {

    private static final char QUOTE = '"';

    private final char separator;
    private final Charset charset;

    public CsvInputReader(MassiveSearchProperties properties) {
        String configured = properties.getCsv().getSeparator();
        this.separator = (configured == null || configured.isEmpty()) ? ',' : configured.charAt(0);
        this.charset = properties.getCsv().getCharset();
    }

    /** Opens a buffered reader over the given stream using the configured charset. */
    public BufferedReader newReader(InputStream inputStream) {
        return new BufferedReader(new InputStreamReader(inputStream, charset));
    }

    /**
     * Opens a buffered reader over the given in-memory CSV content (perimeter stored in the DB).
     * A {@code null} content is treated as empty so callers see an empty (headerless) input.
     */
    public BufferedReader newReader(String content) {
        return new BufferedReader(new StringReader(content == null ? "" : content));
    }

    /** {@code true} when the line is null or contains only whitespace. */
    public boolean isBlank(String line) {
        return !StringUtils.hasText(line);
    }

    /** Parses a single CSV line into its fields, honouring quotes and escaped quotes. */
    public List<String> parseLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == QUOTE) {
                    if (i + 1 < line.length() && line.charAt(i + 1) == QUOTE) {
                        current.append(QUOTE);
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else if (c == QUOTE) {
                inQuotes = true;
            } else if (c == separator) {
                fields.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString());
        return fields;
    }

    /** Normalizes parsed columns into a {@link SearchInputRow} according to the detected template. */
    public SearchInputRow toRow(TemplateDetection detection, List<String> columns) {
        return new SearchInputRow(
            valueAt(columns, detection.navIndex()),
            valueAt(columns, detection.paIndex()),
            valueAt(columns, detection.iuvIndex()),
            valueAt(columns, detection.tokenIndex())
        );
    }

    private String valueAt(List<String> columns, int index) {
        if (index < 0 || index >= columns.size()) {
            return null;
        }
        String value = columns.get(index);
        if (value == null) {
            return null;
        }
        String trimmed = value.replace("\uFEFF", "").trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
