package it.pagopa.cruscotto.ingestion.massivesearch.report;

import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

/**
 * Shared SQL fragment applying the optional analysis window to a token alias.
 * Centralized so the three report repositories share a single definition of the predicate.
 *
 * <p>La finestra e' calcolata su {@code inserted_timestamp} (timestamp della sorgente ADX, sempre
 * valorizzato) e non su {@code payment_date}, che e' null per i token non pagati: filtrando su
 * payment_date i tentativi non pagati sparirebbero dalla ricerca per periodo.</p>
 *
 * <p>Il frammento viene generato <strong>solo per i bound effettivamente presenti</strong>: la
 * precedente forma {@code (CAST(:winFrom AS timestamp) IS NULL OR ...)} restava nel piano anche a
 * finestra assente e impediva al planner di sfruttare il bound come predicato indicizzabile.</p>
 *
 * <p><strong>Nessun predicato viene aggiunto su {@code date_event}</strong> (chiave di
 * partizionamento mensile) per potare le partizioni: sarebbe scorretto. {@code inserted_timestamp}
 * e' scritto solo all'insert (primo evento del token) mentre {@code date_event} viene riscritto da
 * ogni UPDATE con la data dell'ultimo evento, quindi
 * {@code date_event >= date(inserted_timestamp)} con uno scarto pari all'intero ciclo di vita del
 * pagamento. Un bound superiore su {@code date_event} escluderebbe i token pagati/aggiornati dopo
 * la fine della finestra, perdendo righe.</p>
 */
public final class ReportWindowSql {

    private ReportWindowSql() {
    }

    /**
     * Window predicate for the given {@code position_tokens} alias.
     *
     * @param alias      the SQL alias of the {@code position_tokens} row to filter
     * @param window     the analysis window (may be {@code null} or unbounded)
     * @return the {@code AND (...)} fragment to append to a WHERE / JOIN condition, empty when unbounded
     */
    public static String tokenWindow(String alias, AnalysisWindow window) {
        if (window == null || !window.hasBounds()) {
            return "";
        }
        StringBuilder sql = new StringBuilder();
        if (window.fromInclusive() != null) {
            sql.append(" AND ").append(alias).append(".inserted_timestamp >= CAST(:winFrom AS timestamp)");
        }
        if (window.toExclusive() != null) {
            sql.append(" AND ").append(alias).append(".inserted_timestamp < CAST(:winTo AS timestamp)");
        }
        return sql.toString();
    }

    /** Binds the parameters referenced by {@link #tokenWindow}. */
    public static void bind(MapSqlParameterSource params, AnalysisWindow window) {
        if (window == null || !window.hasBounds()) {
            return;
        }
        if (window.fromInclusive() != null) {
            params.addValue("winFrom", window.fromInclusive());
        }
        if (window.toExclusive() != null) {
            params.addValue("winTo", window.toExclusive());
        }
    }
}
