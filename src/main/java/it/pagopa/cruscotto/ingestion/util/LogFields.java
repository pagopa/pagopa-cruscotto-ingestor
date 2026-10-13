package it.pagopa.cruscotto.ingestion.util;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Il dizionario dei campi di log, come prescritto da <em>OER Logging</em>.
 *
 * <p><strong>Perche' un dizionario chiuso.</strong> Le linee guida chiedono di decidere, per ogni
 * valore, se sara' oggetto di <em>ricerche globali, filtri frequenti o correlazioni fra servizi</em>.
 * Se si', e' un attributo di primo livello col prefisso {@code ctx_}; se no, e' una chiave dentro
 * l'oggetto {@code ctx_details}. Tenere quella scelta sparsa fra 280 chiamate a {@code log.*}
 * significa non averla: dopo tre mesi il dizionario di primo livello contiene tutto, che e'
 * esattamente il problema che la regola vuole evitare.
 *
 * <p><strong>Perche' i tipi sono dichiarati qui.</strong> L'MDC di SLF4J e'
 * {@code String -> String}: un contatore messo in MDC arriverebbe a Elasticsearch come
 * {@code "rows": "42"}, cioe' come <em>testo</em>. Il mapping dinamico lo indicizzerebbe come
 * keyword e ogni {@code sum} o {@code avg} su quel campo diventerebbe impossibile — senza errore,
 * semplicemente restituendo zero. Per questo le chiavi numeriche e booleane sono elencate: il
 * customizer le riconverte al tipo giusto prima di scriverle nel JSON.
 *
 * @see CtxDetailsJsonCustomizer il punto unico che applica questa classificazione
 */
public final class LogFields {

    // ─── Primo livello: si filtra e si aggrega su questi ──────────────────────────────────────

    /**
     * L'identificativo che correla tutte le righe di una stessa esecuzione.
     *
     * <p>E' il {@code runId} del job. Le linee guida sono scritte per microservizi
     * request/response e parlano di <em>"identificativo generato all'ingresso del sistema"</em>;
     * in uno scheduler batch non esiste una richiesta in ingresso, e l'equivalente naturale e'
     * l'esecuzione. Mappatura da far validare al cliente.
     *
     * <p>Il campo {@code trace.id} degli esempi del documento <strong>non</strong> viene emesso:
     * non compare nella tabella di mappatura (che prescrive {@code correlation_id}) e in ECS
     * appartiene al tracing distribuito, che qui non esiste. Duplicare il valore sotto un nome
     * che promette una semantica che non abbiamo sarebbe peggio che ometterlo.
     */
    public static final String CORRELATION_ID = "correlation_id";

    /** Il punto di ingresso dell'elaborazione: per noi l'entita' ingerita ({@code POSITION}, ...). */
    public static final String EVENT_ACTION = "event_action";

    /** Esito della milestone. Valori ammessi dal documento: {@code success} / {@code failure}. */
    public static final String EVENT_OUTCOME = "event_outcome";

    /**
     * La fase interna del ciclo di ingestione ({@code WINDOW}, {@code CHECKPOINT}, ...).
     *
     * <p>Sta di primo livello e non in {@code ctx_details} per il criterio del documento: e' il
     * campo su cui si filtra per isolare un passaggio del flusso, non un dettaglio utile alla
     * lettura di un singolo evento.
     */
    public static final String CTX_PHASE = "ctx_phase";

    /** Il pod che ha prodotto la riga: serve a distinguere i tre nodi del cluster Quartz. */
    public static final String CTX_INSTANCE_ID = "ctx_instance_id";

    /** L'esecuzione di ricerca massiva, per seguirne una sola fra le concorrenti. */
    public static final String CTX_EXECUTION_ID = "ctx_execution_id";

    /** La finestra di ingestione dentro l'esecuzione, per correlare le righe di un ciclo. */
    public static final String CTX_OPERATION_ID = "ctx_operation_id";

    /** Attributi di primo livello prodotti da noi, nell'ordine in cui si leggono. */
    public static final List<String> TOP_LEVEL = List.of(
            CORRELATION_ID, EVENT_ACTION, EVENT_OUTCOME, CTX_PHASE,
            CTX_INSTANCE_ID, CTX_EXECUTION_ID, CTX_OPERATION_ID);

    // ─── ctx_details: contesto del singolo evento, non si aggrega su scala ────────────────────

    /** Il sistema oltre il confine di I/O: {@code ADX} oppure {@code postgres}. */
    public static final String DEPENDENCY = "dependency";

    /** Il database interrogato sulla dipendenza. */
    public static final String DATABASE = "database";

    /** Nome della tabella oggetto dell'operazione. */
    public static final String TABLE = "table";

    /** Ampiezza della finestra temporale interrogata, come la esprime la configurazione. */
    public static final String WINDOW = "window";

    /** Numero del tentativo, da 1. */
    public static final String ATTEMPT = "attempt";

    /** Tentativi massimi previsti dalla configurazione per questa operazione. */
    public static final String MAX_ATTEMPTS = "max_attempts";

    /** Righe lette o scritte dall'operazione. */
    public static final String ROWS = "rows";

    /** Durata dell'operazione. */
    public static final String ELAPSED_MS = "elapsed_ms";

    /**
     * Istante di inizio del tentativo, in millisecondi epoch.
     *
     * <p>Insieme a {@link #ENDED_AT_MS} e' il dato con cui si ricostruisce la concorrenza
     * <em>reale</em> imposta a una dipendenza: la legge di Little da la media da una somma, ma il
     * picco richiede gli <em>intervalli</em>. E' servito durante l'incidente ADX del 6-7 ottobre
     * 2026 e va preservato.
     */
    public static final String STARTED_AT_MS = "started_at_ms";

    /** Istante di fine del tentativo, in millisecondi epoch. */
    public static final String ENDED_AT_MS = "ended_at_ms";

    /** Se dopo un esito negativo seguira' un altro tentativo. */
    public static final String WILL_RETRY = "will_retry";

    /**
     * Il guasto della dipendenza, appiattito, quando {@code error.*} non puo' contenerlo.
     *
     * <p><strong>Perche' non basta {@code error.message}.</strong> Il driver Kusto avvolge il
     * fallimento utile in una {@code DataServiceException} generica (<em>"parsing json
     * response... multiple inner exceptions"</em>) ed espone il segnale vero — p.es.
     * {@code E_RS_CANNOT_RETRIEVE_VALUES_ERROR} o {@code LimitsExceeded} — come {@code Iterable}
     * di inner exception, <strong>non</strong> come {@code cause}. L'encoder ricava
     * {@code error.type} / {@code error.message} / {@code error.stack_trace} dalla catena delle
     * cause, quindi di quel segnale non vedrebbe nulla: resterebbe l'involucro generico.
     *
     * <p>Non viene scritto come {@code error.message} di primo livello di proposito: l'encoder
     * emette {@code error} come oggetto annidato, e una chiave piatta omonima darebbe in
     * Elasticsearch un conflitto di mapping fra oggetto e campo concreto — cioe' il rifiuto del
     * documento, perdendo la riga intera invece di un campo.
     */
    public static final String ERROR_DETAIL = "error_detail";

    /** Chiavi di {@code ctx_details} da scrivere come numero. */
    public static final Set<String> DETAILS_NUMERIC = Set.of(
            ATTEMPT, MAX_ATTEMPTS, ROWS, ELAPSED_MS, STARTED_AT_MS, ENDED_AT_MS);

    /** Chiavi di {@code ctx_details} da scrivere come booleano. */
    public static final Set<String> DETAILS_BOOLEAN = Set.of(WILL_RETRY);

    /** Chiavi di {@code ctx_details} da scrivere come stringa. */
    public static final Set<String> DETAILS_TEXT =
            Set.of(DEPENDENCY, DATABASE, TABLE, WINDOW, ERROR_DETAIL);

    /**
     * Tutte le chiavi di {@code ctx_details}, nell'ordine di scrittura.
     *
     * <p>L'ordine e' fissato di proposito: rende la riga di log confrontabile fra esecuzioni e i
     * test deterministici. Un {@code Set.of()} non lo garantirebbe.
     */
    public static final List<String> DETAILS = List.of(
            DEPENDENCY, DATABASE, TABLE, WINDOW,
            ATTEMPT, MAX_ATTEMPTS, ROWS, ELAPSED_MS, STARTED_AT_MS, ENDED_AT_MS, WILL_RETRY,
            ERROR_DETAIL);

    /** Nome dell'oggetto annidato che raccoglie {@link #DETAILS}. */
    public static final String CTX_DETAILS = "ctx_details";

    private LogFields() {
    }

    /**
     * Verifica che la classificazione sia completa e senza sovrapposizioni.
     *
     * <p>Non e' una comodita' per i test: una chiave presente in {@link #DETAILS} ma in nessuno
     * dei tre insiemi tipizzati verrebbe <strong>scartata in silenzio</strong> dal customizer, e
     * una chiave che stesse sia in {@link #TOP_LEVEL} sia in {@link #DETAILS} verrebbe scritta
     * annidata e poi rimossa dal primo livello, cioe' persa a meta'. Entrambi i casi sono
     * invisibili a occhio e si scoprirebbero solo cercando un campo che non c'e'.
     *
     * @return le chiavi mal classificate, vuoto se tutto e' coerente
     */
    public static Set<String> misclassifiedKeys() {
        Set<String> wrong = new LinkedHashSet<>();
        for (String key : DETAILS) {
            boolean typed = DETAILS_NUMERIC.contains(key)
                    || DETAILS_BOOLEAN.contains(key)
                    || DETAILS_TEXT.contains(key);
            if (!typed || TOP_LEVEL.contains(key)) {
                wrong.add(key);
            }
        }
        for (String key : DETAILS_NUMERIC) {
            if (!DETAILS.contains(key)) {
                wrong.add(key);
            }
        }
        for (String key : DETAILS_BOOLEAN) {
            if (!DETAILS.contains(key)) {
                wrong.add(key);
            }
        }
        for (String key : DETAILS_TEXT) {
            if (!DETAILS.contains(key)) {
                wrong.add(key);
            }
        }
        return wrong;
    }
}
