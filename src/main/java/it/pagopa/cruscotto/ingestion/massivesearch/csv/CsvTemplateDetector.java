package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Recognizes the {@link CsvTemplate} of an uploaded CSV from its header row and resolves the column
 * index of each logical field. Header matching is case-insensitive and accepts common synonyms for
 * the {@code PA} column (e.g. {@code idDominio}).
 */
@Component
public class CsvTemplateDetector {

    /** Logical fields a template can carry. */
    public enum Field { NAV, PA, IUV, TOKEN }

    private static final Map<String, Field> FIELD_BY_HEADER = Map.ofEntries(
        Map.entry("nav", Field.NAV),
        Map.entry("iuv", Field.IUV),
        Map.entry("token", Field.TOKEN),
        // idDominio / Ente Creditore column. "EC" is the header used by the client's perimeter CSVs
        // (NAV;EC, IUV;EC); the other spellings are accepted as tolerant synonyms.
        Map.entry("ec", Field.PA),
        Map.entry("pa", Field.PA),
        Map.entry("iddominio", Field.PA),
        Map.entry("id_dominio", Field.PA)
    );

    /**
     * Detection outcome: matched template, per-field column indexes (or {@code -1}), and any
     * unexpected / duplicate header columns.
     */
    public record TemplateDetection(
        CsvTemplate template,
        int navIndex,
        int paIndex,
        int iuvIndex,
        int tokenIndex,
        List<String> unexpectedColumns,
        List<String> duplicateColumns
    ) {}

    /**
     * Detects the template from the parsed header columns.
     *
     * @param headerColumns header cells as parsed from the first line (already unquoted)
     * @return the detection outcome
     */
    public TemplateDetection detect(List<String> headerColumns) {
        Map<Field, Integer> indexes = new EnumMap<>(Field.class);
        List<String> unexpected = new ArrayList<>();
        List<String> duplicates = new ArrayList<>();

        if (headerColumns != null) {
            for (int i = 0; i < headerColumns.size(); i++) {
                String raw = headerColumns.get(i);
                Field field = FIELD_BY_HEADER.get(normalize(raw));
                if (field == null) {
                    unexpected.add(raw == null ? "" : raw.trim());
                } else if (indexes.containsKey(field)) {
                    duplicates.add(raw == null ? "" : raw.trim());
                } else {
                    indexes.put(field, i);
                }
            }
        }

        CsvTemplate template = resolveTemplate(indexes.keySet());
        return new TemplateDetection(
            template,
            indexes.getOrDefault(Field.NAV, -1),
            indexes.getOrDefault(Field.PA, -1),
            indexes.getOrDefault(Field.IUV, -1),
            indexes.getOrDefault(Field.TOKEN, -1),
            unexpected,
            duplicates
        );
    }

    private CsvTemplate resolveTemplate(Set<Field> fields) {
        if (fields.equals(Set.of(Field.NAV, Field.PA))) {
            return CsvTemplate.NAV_PA;
        }
        if (fields.equals(Set.of(Field.IUV, Field.PA))) {
            return CsvTemplate.IUV_PA;
        }
        if (fields.equals(Set.of(Field.NAV))) {
            return CsvTemplate.NAV;
        }
        if (fields.equals(Set.of(Field.IUV))) {
            return CsvTemplate.IUV;
        }
        if (fields.equals(Set.of(Field.TOKEN))) {
            return CsvTemplate.TOKEN;
        }
        return CsvTemplate.UNKNOWN;
    }

    private String normalize(String value) {
        if (!StringUtils.hasText(value)) {
            return "";
        }
        // strip a possible UTF-8 BOM on the first header cell
        String cleaned = value.replace("\uFEFF", "").trim().toLowerCase(Locale.ROOT);
        return cleaned;
    }
}
