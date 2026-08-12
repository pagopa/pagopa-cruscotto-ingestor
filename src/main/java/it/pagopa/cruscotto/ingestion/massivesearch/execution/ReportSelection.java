package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

/**
 * Parses the per-instance report selection persisted on {@code search_instance.selected_reports}
 * as a comma-separated set of {@link ReportType} names ({@code POSITION,TOKEN,TRANSFER}). The value
 * is written by the external API Layer from the GUI checkboxes.
 *
 * <p>A {@code null}/blank value — or one that resolves to no known report — means "all reports",
 * preserving the historical always-three behaviour and keeping the pipeline safe if the external
 * contract ever sends an unexpected value.</p>
 */
public final class ReportSelection {

    private ReportSelection() {
    }

    /**
     * @param csv comma-separated {@link ReportType} names, or {@code null}/blank for "all"
     * @return the selected report types; never empty (falls back to all)
     */
    public static Set<ReportType> parse(String csv) {
        if (csv == null || csv.isBlank()) {
            return EnumSet.allOf(ReportType.class);
        }
        Set<ReportType> selected = EnumSet.noneOf(ReportType.class);
        for (String token : csv.split(",")) {
            String name = token.trim().toUpperCase(Locale.ROOT);
            if (name.isEmpty()) {
                continue;
            }
            try {
                selected.add(ReportType.valueOf(name));
            } catch (IllegalArgumentException ignored) {
                // Unknown report name in the contract string: ignore defensively.
            }
        }
        return selected.isEmpty() ? EnumSet.allOf(ReportType.class) : selected;
    }
}
