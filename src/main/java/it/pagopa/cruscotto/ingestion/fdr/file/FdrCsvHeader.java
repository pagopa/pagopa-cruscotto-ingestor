package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The header of one FDR file, turned into a name to position mapping.
 *
 * <p>Reading by name and not by position is the single decision this class exists for. The two
 * datasets share eight column names in different positions, so a positional reader that was handed
 * the wrong dataset — or a file whose columns the source reordered — would not fail: it would write
 * an {@code IUR} into an {@code IUV}. By name, the same accident is a missing column and stops the
 * file.
 *
 * <p>Matching is case-insensitive and ignores surrounding whitespace in the <em>header</em> only.
 * That is not a contradiction of the source-fidelity rule, which is about values: a header is
 * metadata, and refusing a day because a column arrived as {@code "iuv "} would be strictness
 * pointed at the wrong target.
 *
 * <p>Columns the dataset does not expect are kept, not rejected: the source is free to add a field,
 * and losing a day over a column nobody reads would be the worse failure. They are reported so that
 * the addition is noticed rather than discovered months later.
 */
public final class FdrCsvHeader {

    private final Map<String, Integer> indexByName;
    private final int columnCount;
    private final List<String> unexpectedColumns;

    private FdrCsvHeader(Map<String, Integer> indexByName, int columnCount,
                         List<String> unexpectedColumns) {
        this.indexByName = indexByName;
        this.columnCount = columnCount;
        this.unexpectedColumns = List.copyOf(unexpectedColumns);
    }

    /**
     * Builds the mapping, validating it against the dataset.
     *
     * @param cells   the header line, already split by {@link FdrCsvParser} and with the BOM removed
     * @param dataset the dataset the file was classified as, from its name
     * @throws FdrFileFormatException with {@link FdrFileFormatException.Kind#UNEXPECTED_HEADER} when
     *                                a required column is missing or a name appears twice
     */
    public static FdrCsvHeader of(List<String> cells, FdrDataset dataset) {
        Map<String, Integer> indexByName = new HashMap<>();
        Set<String> duplicates = new LinkedHashSet<>();

        for (int i = 0; i < cells.size(); i++) {
            String name = normalize(cells.get(i));
            // The first occurrence wins the position, but a duplicate is still fatal: the file
            // declares two columns with one name and nothing can say which one carries the data.
            if (indexByName.putIfAbsent(name, i) != null) {
                duplicates.add(name);
            }
        }

        List<String> required = FdrColumns.required(dataset);
        List<String> missing = new ArrayList<>();
        for (String column : required) {
            if (!indexByName.containsKey(column)) {
                missing.add(column);
            }
        }
        if (!missing.isEmpty() || !duplicates.isEmpty()) {
            throw new FdrFileFormatException(FdrFileFormatException.Kind.UNEXPECTED_HEADER,
                    "FDR " + dataset + " header does not match: missing " + missing
                            + ", duplicated " + duplicates);
        }

        List<String> unexpected = new ArrayList<>(indexByName.keySet());
        unexpected.removeAll(required);
        unexpected.sort(String::compareTo);

        return new FdrCsvHeader(Map.copyOf(indexByName), cells.size(), unexpected);
    }

    private static String normalize(String cell) {
        return cell == null ? "" : cell.trim().toUpperCase(Locale.ROOT);
    }

    /** How many columns the header declared. Rows are expected to carry exactly this many fields. */
    public int columnCount() {
        return columnCount;
    }

    /** Columns present in the file but not expected for the dataset, sorted. Never {@code null}. */
    public List<String> unexpectedColumns() {
        return unexpectedColumns;
    }

    /**
     * The value of one column in one row, or {@code null} when the field is empty.
     *
     * <p>Empty becomes {@code null} and nothing else happens to the text: no trimming, no case
     * change, no shortening. An empty CSV field and a missing one are the same thing to the
     * database, which holds every source column nullable.
     *
     * @throws FdrValueException if the row has fewer fields than the header declared. Raised here
     *                           rather than counted as a shorter row because reading the columns
     *                           that <em>are</em> present would mean trusting their position, which
     *                           a truncated line is exactly what invalidates.
     */
    public String value(List<String> fields, String column) {
        Integer index = indexByName.get(column);
        if (index == null) {
            // Unreachable through the mappers, which only ask for required columns, but a caller
            // asking for a column this header does not have is a defect worth naming.
            throw new FdrValueException(column, "column is not present in the header");
        }
        if (index >= fields.size()) {
            throw new FdrValueException(column,
                    "row has " + fields.size() + " fields, header declares " + columnCount);
        }
        String raw = fields.get(index);
        return raw == null || raw.isEmpty() ? null : raw;
    }
}
