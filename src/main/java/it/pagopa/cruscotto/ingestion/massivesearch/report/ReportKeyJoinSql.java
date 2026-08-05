package it.pagopa.cruscotto.ingestion.massivesearch.report;

import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Builds the set-based {@code JOIN (VALUES ...) AS k(...)} clause that filters {@code position p} to
 * the rows matching any of a batch of perimeter keys, replacing the former one-query-per-key access.
 *
 * <p>Shared by the three report repositories so the key-matching semantics (case-insensitive
 * {@code LOWER} on NAV/PA/IUV, {@code decode('hex')} on TOKEN) stay identical. Every value is bound as
 * a named parameter and cast to {@code text} so PostgreSQL can infer the VALUES column types. Keys
 * missing the fields required by the template are skipped; when no valid key remains (or the template
 * is {@link CsvTemplate#UNKNOWN}) the method returns {@code null} and the caller must skip the query.</p>
 */
public final class ReportKeyJoinSql {

    private ReportKeyJoinSql() {
    }

    /**
     * @param template the CSV template driving key resolution
     * @param schema   the resolved DB schema (used to qualify {@code position_tokens})
     * @param keys     the batch of input keys
     * @param params   parameter source that receives the bound key values
     * @return the {@code JOIN (VALUES ...) ON ...} clause, or {@code null} when no query should run
     */
    public static String buildKeyJoin(CsvTemplate template, String schema,
                                      List<SearchInputRow> keys, MapSqlParameterSource params) {
        String tokens = schema + ".position_tokens";
        return switch (template) {
            case NAV_PA -> pairJoin(keys, params, "nav", "pa",
                k -> hasText(k.nav()) && hasText(k.pa()),
                k -> new String[]{k.nav(), k.pa()},
                "LOWER(p.nav) = LOWER(k.nav) AND LOWER(p.pa_emittente) = LOWER(k.pa)");
            case NAV -> singleJoin(keys, params, "nav",
                k -> hasText(k.nav()), SearchInputRow::nav,
                "LOWER(p.nav) = LOWER(k.nav)");
            case IUV_PA -> pairJoin(keys, params, "pa", "iuv",
                k -> hasText(k.iuv()) && hasText(k.pa()),
                k -> new String[]{k.pa(), k.iuv()},
                "LOWER(p.pa_emittente) = LOWER(k.pa) AND EXISTS (SELECT 1 FROM " + tokens
                    + " tkf WHERE tkf.fk_position = p.id AND LOWER(tkf.iuv) = LOWER(k.iuv))");
            case IUV -> singleJoin(keys, params, "iuv",
                k -> hasText(k.iuv()), SearchInputRow::iuv,
                "EXISTS (SELECT 1 FROM " + tokens
                    + " tkf WHERE tkf.fk_position = p.id AND LOWER(tkf.iuv) = LOWER(k.iuv))");
            case TOKEN -> singleJoin(keys, params, "token",
                k -> hasText(k.token()), SearchInputRow::token,
                "EXISTS (SELECT 1 FROM " + tokens
                    + " tkf WHERE tkf.fk_position = p.id AND tkf.token = decode(k.token, 'hex'))");
            case UNKNOWN -> null;
        };
    }

    private static String singleJoin(List<SearchInputRow> keys, MapSqlParameterSource params,
                                     String column, Predicate<SearchInputRow> valid,
                                     Function<SearchInputRow, String> extract, String on) {
        List<String> tuples = new ArrayList<>();
        int i = 0;
        for (SearchInputRow key : keys) {
            if (!valid.test(key)) {
                continue;
            }
            String p = "kj_" + i++;
            params.addValue(p, extract.apply(key));
            tuples.add("(CAST(:" + p + " AS text))");
        }
        if (tuples.isEmpty()) {
            return null;
        }
        return "JOIN (VALUES " + String.join(",", tuples) + ") AS k(" + column + ") ON " + on;
    }

    private static String pairJoin(List<SearchInputRow> keys, MapSqlParameterSource params,
                                   String column0, String column1, Predicate<SearchInputRow> valid,
                                   Function<SearchInputRow, String[]> extract, String on) {
        List<String> tuples = new ArrayList<>();
        int i = 0;
        for (SearchInputRow key : keys) {
            if (!valid.test(key)) {
                continue;
            }
            String p0 = "kj_" + i + "_0";
            String p1 = "kj_" + i + "_1";
            i++;
            String[] values = extract.apply(key);
            params.addValue(p0, values[0]);
            params.addValue(p1, values[1]);
            tuples.add("(CAST(:" + p0 + " AS text), CAST(:" + p1 + " AS text))");
        }
        if (tuples.isEmpty()) {
            return null;
        }
        return "JOIN (VALUES " + String.join(",", tuples) + ") AS k(" + column0 + ", " + column1 + ") ON " + on;
    }

    private static boolean hasText(String value) {
        return StringUtils.hasText(value);
    }
}
