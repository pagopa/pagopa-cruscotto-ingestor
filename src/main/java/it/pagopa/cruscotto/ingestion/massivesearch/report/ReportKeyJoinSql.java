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
 * <p>Shared by the three report repositories so the key-matching semantics stay identical. Matching is
 * an <strong>exact equality</strong> on NAV/PA/IUV/TOKEN: NAV and EC/PA are
 * numeric codes (case is irrelevant) while IUV and TOKEN are case-sensitive per the client spec, so no
 * {@code LOWER} is applied. Keeping the predicates free of functions on the indexed columns lets
 * PostgreSQL use the plain b-tree indexes ({@code position(nav, pa_emittente)},
 * {@code position_tokens(iuv)}, {@code position_tokens(token)}) instead of falling back to a sequential
 * scan. Every value is bound as a named parameter and cast to {@code text} so PostgreSQL can infer the
 * VALUES column types. Keys missing the fields required by the template are skipped; when no valid key
 * remains (or the template is {@link CsvTemplate#UNKNOWN}) the method returns {@code null} and the
 * caller must skip the query.</p>
 *
 * <p>I template risolti sui token (IUV, IUV_PA, TOKEN) sono espressi come join esplicita su
 * {@code position_tokens} e non piu' come {@code EXISTS} correlata su {@code p.id}: nella forma
 * correlata nessun predicato insisteva su {@code position}, che diventava la tabella guida (milioni
 * di righe, 25 partizioni) e rendeva il piano dipendente dalla capacita' del planner di riscrivere
 * la subquery in semi-join. Con la join diretta l'indice sul token guida sempre la ricerca.</p>
 *
 * <p><strong>La cardinalita' e' identica alla forma precedente</strong>: il {@code DISTINCT} proietta
 * la posizione <em>insieme alle colonne chiave</em>, quindi emette esattamente una riga per ogni
 * coppia (posizione, chiave) che prima superava l'{@code EXISTS} — inclusi i casi in cui due chiavi
 * distinte risolvono alla stessa posizione (es. due token dello stesso pagamento). Il {@code DISTINCT}
 * si limita a collassare i token multipli che condividono la stessa chiave, che nella semi-join
 * {@code EXISTS} non duplicavano. Le chiavi identiche sono gia' deduplicate a monte da
 * {@code PerimeterCsvReader}.</p>
 *
 * <p><strong>TOKEN</strong>: la colonna {@code position_tokens.token} e' {@code BYTEA} per eredita'
 * del modello sorgente, ma il contenuto e' sempre testo — l'ingestion scrive {@code getBytes(UTF_8)}
 * della stringa di origine e i report rileggono con {@code convert_from(token, 'UTF8')}. Il confronto
 * deve quindi usare {@code convert_to(chiave, 'UTF8')}: una {@code decode(chiave, 'hex')} non
 * combacerebbe mai (byte diversi) e solleverebbe errore sui token non esadecimali. La funzione sta sul
 * lato costante del predicato, quindi l'indice {@code position_tokens(token)} resta utilizzabile.</p>
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
                "p.nav = k.nav AND p.pa_emittente = k.pa");
            case NAV -> singleJoin(keys, params, "nav",
                k -> hasText(k.nav()), SearchInputRow::nav,
                "p.nav = k.nav");
            case IUV_PA -> tokenPairJoin(keys, params, tokens, "pa", "iuv",
                k -> hasText(k.iuv()) && hasText(k.pa()),
                k -> new String[]{k.pa(), k.iuv()},
                "tkf.iuv = kv.iuv",
                "p.id = k.fk_position AND p.pa_emittente = k.pa");
            case IUV -> tokenJoin(keys, params, tokens, "iuv",
                k -> hasText(k.iuv()), SearchInputRow::iuv,
                "tkf.iuv = kv.iuv");
            case TOKEN -> tokenJoin(keys, params, tokens, "token",
                k -> hasText(k.token()), SearchInputRow::token,
                "tkf.token = convert_to(kv.token, 'UTF8')");
            case UNKNOWN -> null;
        };
    }

    /**
     * Key resolution driven by {@code position_tokens}: the keys are joined to the tokens table
     * (indexed on {@code iuv} / {@code token}) and only the resulting position ids reach
     * {@code position}. The key column is kept in the projection so the (position, key) cardinality
     * of the former {@code EXISTS} semi-join is preserved.
     */
    private static String tokenJoin(List<SearchInputRow> keys, MapSqlParameterSource params,
                                    String tokens, String column,
                                    Predicate<SearchInputRow> valid,
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
        return "JOIN (SELECT DISTINCT tkf.fk_position AS fk_position, kv." + column + " AS " + column
            + " FROM " + tokens + " tkf"
            + " JOIN (VALUES " + String.join(",", tuples) + ") AS kv(" + column + ") ON " + on
            + ") k ON p.id = k.fk_position";
    }

    /** Token-driven resolution carrying an extra key column matched on {@code position} (IUV_PA). */
    private static String tokenPairJoin(List<SearchInputRow> keys, MapSqlParameterSource params,
                                        String tokens, String column0, String column1,
                                        Predicate<SearchInputRow> valid,
                                        Function<SearchInputRow, String[]> extract,
                                        String tokenOn, String positionOn) {
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
        return "JOIN (SELECT DISTINCT tkf.fk_position AS fk_position, kv." + column0 + " AS " + column0
            + ", kv." + column1 + " AS " + column1
            + " FROM " + tokens + " tkf"
            + " JOIN (VALUES " + String.join(",", tuples) + ") AS kv(" + column0 + ", " + column1 + ") ON " + tokenOn
            + ") k ON " + positionOn;
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
