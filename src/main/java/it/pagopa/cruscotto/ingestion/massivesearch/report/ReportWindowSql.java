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
 * (EntityTransformerImpl, entrambe in UTC), l'insert e' registry-gated quindi la riga token nasce
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
     * implicato dal primo — {@code EntityTransformerImpl} deriva entrambi dallo stesso istante UTC.</p>
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
     * <p><strong>Il bound e' asimmetrico: un figlio non puo' precedere il proprio padre.</strong>
     * Un token si aggancia solo a una posizione nata nelle 24 ore <em>precedenti</em>
     * ({@code PositionRepository#findLatestByBusinessKeyWithin24h}), i transfer nascono dallo stesso
     * evento {@code activatePaymentNotice} del token, e le extra info seguono il token entro la durata
     * della sessione di pagamento (confermato dal cliente: ~1h, oltre la sessione scade e l'utente
     * rifa' il tentativo). Il lato inferiore e' quindi la data del padre stessa, senza margine: un
     * bound simmetrico raddoppiava l'ampiezza della finestra senza coprire alcun caso reale.</p>
     *
     * <p>Spread misurato in produzione, coerente con quanto sopra: 0 per i transfer, 0..1 per gli
     * extra_info, <strong>mai negativo</strong>. Il margine in avanti copre il solo passaggio di
     * mezzanotte; resta configurabile perche' <strong>un bound troppo stretto non rallenta, esclude
     * righe dal report in silenzio</strong>. Vedi
     * {@code MassiveSearchProperties.Execution#childDateMarginDays}.</p>
     *
     * @param childAlias alias della tabella figlia partizionata
     * @param tokenAlias alias del padre da cui derivare la finestra
     * @param marginDays giorni ammessi <em>dopo</em> la data del padre; {@code <= 0} disattiva il bound
     */
    public static String childOfToken(String childAlias, String tokenAlias, int marginDays) {
        if (marginDays <= 0) {
            return "";
        }
        return " AND " + childAlias + ".date_event >= " + tokenAlias + ".date_event"
            + " AND " + childAlias + ".date_event <= " + tokenAlias + ".date_event + CAST(:childMarginDays AS integer)";
    }

    /**
     * Bound <strong>costante</strong> inferiore sulla tabella figlia, derivato dalla finestra.
     *
     * <p>Complementare ai bound correlati: quelli potano a <em>runtime</em> (i subpiani non coinvolti
     * risultano {@code never executed}, ma l'{@code Append} resta nel piano), questo e' composto da
     * sole costanti e fa scartare le partizioni <em>in planning</em>. Il driver gira con
     * {@code prepareThreshold=0}, quindi vede i valori reali dei parametri e il pruning e' effettivo.</p>
     *
     * <p><strong>Solo l'estremo inferiore.</strong> {@code child.date_event >= token.date_event >=
     * date(winFrom)} e' sempre vero dove la finestra insiste sul token. Il simmetrico superiore
     * ({@code <= date(winTo) + margine}) sarebbe invece <em>sbagliato</em>: i predicati si sommano in
     * AND, quindi annullerebbe il lavoro di {@link #childOfTokenUpTo} escludendo di nuovo le righe
     * create da una {@code sendPaymentOutcome} tardiva, che per definizione cadono dopo la finestra.</p>
     *
     * <p><strong>Applicabile solo dove la finestra insiste sul token padre</strong> (report Tentativi e
     * Transfer). Nel report Position la finestra seleziona le <em>posizioni</em> e il token non e'
     * finestrato — {@code TOKEN_COUNT} e' overall per requisito — quindi anche questo bound
     * escluderebbe righe a torto.</p>
     *
     * @param childAlias alias della tabella figlia partizionata
     * @param window     finestra di analisi applicata al token padre
     * @param marginDays {@code <= 0} disattiva il bound, coerentemente con gli altri
     */
    public static String childOfWindowStart(String childAlias, AnalysisWindow window, int marginDays) {
        if (marginDays <= 0 || window == null || window.fromInclusive() == null) {
            return "";
        }
        return " AND " + childAlias + ".date_event >= CAST(:winFrom AS date)";
    }

    /** Binds the parameter referenced by {@link #childOfToken}. */
    public static void bindChildMargin(MapSqlParameterSource params, int marginDays) {
        if (marginDays > 0) {
            params.addValue("childMarginDays", marginDays);
        }
    }

    /** Data ISO valida: stesso vincolo applicato in scrittura da {@code PositionDateEventsSql}. */
    private static final String ISO_DATE_PATTERN = "^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$";

    /**
     * Ultima data in cui <em>qualcosa</em> ha toccato la posizione: il massimo fra la sua
     * {@code date_event} (nascita) e i giorni registrati in {@code date_events}.
     *
     * <p>Serve come bound superiore esatto per le tabelle figlie del token. Una
     * {@code sendPaymentOutcome} tardiva non crea una nuova riga token (aggiorna quella esistente), ma
     * <strong>puo' creare righe {@code extra_info}</strong> con {@code fk_token} del token vecchio e
     * {@code date_event} della data tardiva: un margine fisso di pochi giorni le escluderebbe in
     * silenzio, svuotando {@code ADD_INFO_RRN} / {@code ADD_INFO_TID}. Quella stessa SPO tocca pero' la
     * posizione, quindi {@code PositionEventUpdateService} ne appende il giorno a {@code date_events}:
     * questa espressione lo recupera, permettendo di tenere il margine stretto senza perdere righe.</p>
     *
     * <p>Il filtro sul formato e' indispensabile: un cast diretto a {@code date} su un elemento
     * malformato fallirebbe a runtime abortendo l'intera query (stessa ragione per cui
     * {@code PositionDateEventsSql} valida con sole operazioni su stringa). {@code GREATEST} ignora i
     * NULL e l'array vuoto produce NULL, quindi il risultato degrada alla sola {@code date_event}.</p>
     *
     * @param positionAlias alias della riga {@code position}
     * @return espressione SQL di tipo {@code date}
     */
    public static String positionLastDate(String positionAlias) {
        String array = "CASE WHEN jsonb_typeof(" + positionAlias + ".date_events) = 'array'"
            + " THEN " + positionAlias + ".date_events ELSE '[]'::jsonb END";
        return "GREATEST(" + positionAlias + ".date_event, COALESCE((SELECT MAX(de.d::date)"
            + " FROM jsonb_array_elements_text(" + array + ") AS de(d)"
            + " WHERE de.d ~ '" + ISO_DATE_PATTERN + "'), " + positionAlias + ".date_event))";
    }

    /**
     * Bound correlato per una tabella figlia del token, con estremo superiore <strong>esatto</strong>
     * derivato dalla posizione invece che da un margine fisso.
     *
     * <p>Il lato inferiore resta la data del token (un figlio non puo' precedere il padre); il lato
     * superiore e' l'ultima data che ha toccato la posizione ({@link #positionLastDate}) piu' il
     * margine, che qui copre il solo passaggio di mezzanotte.</p>
     *
     * @param childAlias     alias della tabella figlia partizionata
     * @param tokenAlias     alias del token padre
     * @param parentLastDate espressione dell'ultima data della posizione del token
     * @param marginDays     giorni di margine oltre tale data; {@code <= 0} disattiva il bound
     */
    public static String childOfTokenUpTo(String childAlias, String tokenAlias, String parentLastDate,
                                          int marginDays) {
        if (marginDays <= 0) {
            return "";
        }
        return " AND " + childAlias + ".date_event >= " + tokenAlias + ".date_event"
            + " AND " + childAlias + ".date_event <= " + parentLastDate + " + CAST(:childMarginDays AS integer)";
    }
}
