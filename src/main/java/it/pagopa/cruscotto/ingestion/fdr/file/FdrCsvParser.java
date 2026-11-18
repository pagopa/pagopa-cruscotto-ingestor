package it.pagopa.cruscotto.ingestion.fdr.file;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits one CSV line of an FDR file into its fields.
 *
 * <p>Pure and static: no configuration, no state, no stream. One line in, the fields out. It is
 * where every quoting decision lives, which is why the tests sit on this class rather than on the
 * reader.
 *
 * <p><strong>Why FDR has its own splitter instead of reusing the Massive Search one.</strong> That
 * component is a Spring bean bound to the Massive Search configuration, whose separator is
 * {@code ;}, and it trims every value and turns blanks into {@code null} as part of the same call.
 * FDR needs a comma and must <em>not</em> transform what it reads. What remains in common is twenty
 * lines of state machine, and duplicating those is cheaper than coupling two bounded contexts
 * through a bean whose configuration one of them must then override.
 *
 * <p><strong>Mixed quoting is the case that matters.</strong> The production exports quote strings
 * and leave numbers bare, and not even consistently across files of the same day: the same
 * {@code COD_ESITO} arrives as {@code "0"} in one file and {@code 0} in another. A splitter that
 * assumed uniform quoting would mis-align the fields of half the files — not fail, mis-align, which
 * is the kind of defect that reaches the database looking plausible.
 */
public final class FdrCsvParser {

    /** The separator of the FDR exports. Measured, not configured: it is a comma in every file. */
    public static final char SEPARATOR = ',';

    static final char QUOTE = '"';

    /** UTF-8 byte order mark, as a character once decoded. Present in some files, absent in others. */
    static final char BOM = '﻿';

    private FdrCsvParser() {
    }

    /**
     * Removes the byte order mark from the first line of a file, if present.
     *
     * <p>Only ever applied to the header: a BOM is a file-level marker, and stripping it from every
     * line would silently delete a character that legitimately appeared inside a value.
     */
    public static String stripBom(String line) {
        return line != null && !line.isEmpty() && line.charAt(0) == BOM ? line.substring(1) : line;
    }

    /**
     * Splits one line.
     *
     * <p>Rules, all of them observed in the real files:
     * <ul>
     *   <li>a field may be quoted or bare, independently of its neighbours;</li>
     *   <li>inside a quoted field, {@code ""} is a literal quote and a separator is just a
     *       character — which is how {@code ALL_DATES} carries a JSON array;</li>
     *   <li>an empty field yields an empty string. Turning it into {@code null} is the mapper's
     *       decision, not the splitter's;</li>
     *   <li>a quote inside a bare field is kept verbatim: it is not valid CSV, but the source has
     *       no reason to produce it and discarding characters would be worse than keeping them.
     *       For the same reason, anything following a closing quote joins the same field instead of
     *       failing the row.</li>
     * </ul>
     *
     * @throws FdrMalformedLineException if a quoted field is never closed. This is the one case the
     *                                   splitter cannot resolve on its own: either the file uses
     *                                   multi-line fields — none of the 810.492 profiled rows did —
     *                                   or the line is corrupt. Guessing would shift every
     *                                   subsequent field by one.
     */
    public static List<String> parseLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c != QUOTE) {
                    current.append(c);
                } else if (i + 1 < line.length() && line.charAt(i + 1) == QUOTE) {
                    current.append(QUOTE);
                    i++;
                } else {
                    inQuotes = false;
                }
            } else if (c == QUOTE && current.length() == 0) {
                // A quote opens a field only at its start. Anywhere else it is content: see the
                // bare-field rule above.
                inQuotes = true;
            } else if (c == SEPARATOR) {
                fields.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        if (inQuotes) {
            throw new FdrMalformedLineException("unterminated quoted field");
        }
        fields.add(current.toString());
        return fields;
    }
}
