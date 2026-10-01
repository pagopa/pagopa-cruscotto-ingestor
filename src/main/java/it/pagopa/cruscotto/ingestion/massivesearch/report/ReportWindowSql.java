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
 * <p><strong>Partition pruning.</strong> Accanto a ogni bound su {@code inserted_timestamp} viene
 * emesso un bound ridondante su {@code date_event}, chiave di partizionamento mensile. Serve solo a
 * far scartare al planner le partizioni non coinvolte: senza, ogni LATERAL su {@code position_tokens}
 * apre tutte le ~25 partizioni (misurati ~52 buffer per loop, in larga parte su partizioni vuote).</p>
 *
 * <p>Il predicato e' esatto, non euristico: {@code date_event} e' derivata da
 * {@code inserted_timestamp} nello stesso istante e dallo stesso valore sorgente ADX
 * (PositionTokensTransformer, entrambe in UTC), l'insert e' registry-gated quindi la riga token nasce
 * una volta sola, e l'UPDATE non tocca piu' nessuna delle due (BulkWriterImpl). Vale quindi
 * l'invariante {@code date_event = date(inserted_timestamp)} e nessun margine e' necessario.
 * Verificato su produzione: scarto 0 su 83M righe, nessuna riga migrata di partizione.</p>
 *
 * <p>Se un giorno {@code DATE_EVENT} tornasse nella SET dell'UPDATE di POSITION_TOKENS l'invariante
 * cadrebbe e questi bound andrebbero rimossi o dotati di margine: perderebbero righe in silenzio.</p>
 *
 * <p>La LATERAL del {@code token_count} resta volutamente priva di finestra (il conteggio e' overall
 * per requisito del cliente) e quindi non e' potabile.</p>
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
        return window(alias, window);
    }

    /**
     * Window predicate for the given {@code position} alias.
     *
     * <p>Nel report Position la finestra di ricerca seleziona le <strong>posizioni</strong>, non i
     * tentativi: una posizione presente nel periodo va riportata anche se il suo unico tentativo cade
     * mesi dopo (requisito cliente). I report Tentativi e Transfer restano invece finestrati sul
     * token via {@link #tokenWindow}.</p>
     *
     * <p>Stessa coppia di predicati del token: {@code inserted_timestamp} porta la precisione al
     * secondo richiesta dall'utente, {@code date_event} serve solo al partition pruning ed e'
     * implicato dal primo — {@code PositionTransformer} deriva entrambi dallo stesso istante UTC.</p>
     */
    public static String positionWindow(String alias, AnalysisWindow window) {
        return window(alias, window);
    }

    private static String window(String alias, AnalysisWindow window) {
        if (window == null || !window.hasBounds()) {
            return "";
        }
        StringBuilder sql = new StringBuilder();
        if (window.fromInclusive() != null) {
            sql.append(" AND ").append(alias).append(".inserted_timestamp >= CAST(:winFrom AS timestamp)");
            // Bound di pruning: date_event = date(inserted_timestamp), quindi implicato dal predicato
            // sopra e mai piu' restrittivo. Il troncamento a date allarga, non stringe.
            sql.append(" AND ").append(alias).append(".date_event >= CAST(:winFrom AS date)");
        }
        if (window.toExclusive() != null) {
            sql.append(" AND ").append(alias).append(".inserted_timestamp < CAST(:winTo AS timestamp)");
            // '<=' e non '<': winTo e' esclusivo sul timestamp ma la sua troncatura a date e' l'ultimo
            // giorno ammissibile, che va incluso (es. winTo = 26-09 00:00 -> date_event max = 25-09).
            sql.append(" AND ").append(alias).append(".date_event <= CAST(:winTo AS date)");
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

    /**
     * Bound correlato che lega la {@code date_event} di una tabella figlia a quella del token padre,
     * per consentire il <em>runtime partition pruning</em> nel Nested Loop.
     *
     * <p>Le LATERAL su {@code position_transfers} / {@code extra_info} sono correlate solo su
     * {@code fk_token} e non hanno finestra temporale: senza questo bound ogni lookup apre tutte le
     * ~25 partizioni mensili per leggere 1-2 righe. Il predicato non dipende dalla finestra di
     * analisi, quindi migliora anche le ricerche senza periodo.</p>
     *
     * <p><strong>Non e' un invariante ma una relazione empirica</strong> (spread misurato in
     * produzione: 0 per i transfer, 0..1 per gli extra_info, mai negativo): il margine va tenuto
     * largo, perche' un bound troppo stretto non rallenta ma esclude righe dal report. Vedi
     * {@code MassiveSearchProperties.Execution#childDateMarginDays}.</p>
     *
     * @param childAlias alias della tabella figlia partizionata
     * @param tokenAlias alias del token padre da cui derivare la finestra
     * @param marginDays margine simmetrico in giorni; {@code <= 0} disattiva il bound
     */
    public static String childOfToken(String childAlias, String tokenAlias, int marginDays) {
        if (marginDays <= 0) {
            return "";
        }
        return " AND " + childAlias + ".date_event >= " + tokenAlias + ".date_event - CAST(:childMarginDays AS integer)"
            + " AND " + childAlias + ".date_event <= " + tokenAlias + ".date_event + CAST(:childMarginDays AS integer)";
    }

    /** Binds the parameter referenced by {@link #childOfToken}. */
    public static void bindChildMargin(MapSqlParameterSource params, int marginDays) {
        if (marginDays > 0) {
            params.addValue("childMarginDays", marginDays);
        }
    }
}
