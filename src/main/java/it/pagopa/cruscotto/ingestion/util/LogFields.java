package it.pagopa.cruscotto.ingestion.util;

import java.util.List;

/**
 * Il dizionario dei campi di log, come prescritto da <em>OER Logging</em>.
 *
 * <p><strong>La scelta che il documento impone.</strong> Per ogni valore va deciso se sara'
 * oggetto di <em>ricerche globali, filtri frequenti o correlazioni fra servizi</em>: se si', e' un
 * attributo di primo livello col prefisso {@code ctx_}; se no, e' una chiave dentro l'oggetto
 * {@code ctx_details}. Qui il primo livello e' un elenco <strong>chiuso</strong> — sono sette
 * campi, e aggiungerne uno e' una decisione — mentre i dettagli sono aperti, perche' ogni
 * milestone ha i suoi.
 *
 * <p><strong>Perche' i dettagli portano il tipo nel nome della chiave.</strong> L'MDC di SLF4J e'
 * {@code String -> String}, ma deve restare il vettore: logback <em>copia</em> l'MDC dentro
 * l'evento, quindi sopravvive a un appender asincrono, cosa che un {@code ThreadLocal} letto in
 * fase di encode non farebbe. Il tipo va percio' trasportato dentro la chiave. L'alternativa —
 * una tabella chiave/tipo — e' stata scartata dopo averla scritta: i punti di log di questo
 * servizio portano circa un centinaio di valori distinti, e un registro di quella dimensione
 * sarebbe rimasto indietro al primo campo nuovo, cioe' avrebbe scritto un numero come stringa
 * proprio nel caso che nessuno controlla.
 *
 * <p>Perche' il tipo conta: un contatore lasciato stringa viene indicizzato da Elasticsearch come
 * keyword, e ogni {@code sum} o {@code avg} su quel campo restituisce <strong>zero senza
 * errore</strong>. Col prefisso il tipo lo decide il tipo Java al punto di chiamata, quindi non
 * c'e' nulla da ricordare.
 *
 * @see LogDetails come si scrivono i dettagli
 * @see CtxDetailsJsonCustomizer come tornano a essere un oggetto JSON tipizzato
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

    /**
     * Il token di pagamento (CCP) a cui la riga si riferisce.
     *
     * <p>E' di primo livello perche' e' un <strong>business ID</strong>, e le linee guida li
     * trattano esplicitamente come il caso tipico dell'attributo indicizzato: e' la chiave con cui
     * si ricostruisce il ciclo di vita di un pagamento attraverso i servizi. IUV, NAV, CCP e
     * {@code transactionId} sono dati attesi nei log, a differenza di CF, email e IBAN, che non
     * devono comparire mai.
     */
    public static final String CTX_TOKEN = "ctx_token";

    /** La chiave dell'evento sorgente, per risalire al record che ha prodotto la riga. */
    public static final String CTX_UNIQUE_ID = "ctx_unique_id";

    /** Attributi di primo livello prodotti da noi, nell'ordine in cui si leggono. */
    public static final List<String> TOP_LEVEL = List.of(
            CORRELATION_ID, EVENT_ACTION, EVENT_OUTCOME, CTX_PHASE,
            CTX_INSTANCE_ID, CTX_EXECUTION_ID, CTX_OPERATION_ID, CTX_TOKEN, CTX_UNIQUE_ID);

    // ─── ctx_details: contesto del singolo evento ─────────────────────────────────────────────

    /** Nome dell'oggetto annidato che raccoglie i dettagli. */
    public static final String CTX_DETAILS = "ctx_details";

    /** Prefisso delle chiavi MDC che trasportano un dettaglio testuale. */
    public static final String DETAIL_TEXT_PREFIX = "d.s.";

    /** Prefisso delle chiavi MDC che trasportano un dettaglio numerico. */
    public static final String DETAIL_NUMERIC_PREFIX = "d.n.";

    /** Prefisso delle chiavi MDC che trasportano un dettaglio booleano. */
    public static final String DETAIL_BOOLEAN_PREFIX = "d.b.";

    /** Tutti i prefissi dei dettagli. */
    public static final List<String> DETAIL_PREFIXES =
            List.of(DETAIL_TEXT_PREFIX, DETAIL_NUMERIC_PREFIX, DETAIL_BOOLEAN_PREFIX);

    // ─── Nomi di dettaglio ricorrenti ─────────────────────────────────────────────────────────
    //
    // Non e' un registro di tipi — quello non serve piu' — ma l'elenco dei nomi usati in piu' di
    // un punto, perche' scriverli a mano due volte in due modi diversi (rows / row_count) rende
    // le due righe non confrontabili.

    /** Il sistema oltre il confine di I/O: {@code ADX} oppure {@code postgres}. */
    public static final String DEPENDENCY = "dependency";

    /** Il database interrogato sulla dipendenza. */
    public static final String DATABASE = "database";

    /** Nome della tabella oggetto dell'operazione. */
    public static final String TABLE = "table";

    /** Ampiezza della finestra temporale interrogata. */
    public static final String WINDOW = "window";

    /** Numero del tentativo, da 1. */
    public static final String ATTEMPT = "attempt";

    /** Tentativi massimi previsti dalla configurazione. */
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

    /** Il motivo per cui una fase si e' fermata o e' stata saltata. */
    public static final String REASON = "reason";

    /** Il cursore di ingestione. */
    public static final String CURSOR = "cursor";

    /** Il limite superiore della finestra di ingestione. */
    public static final String END_LIMIT = "end_limit";

    /** Estremo inferiore, incluso, di una finestra. */
    public static final String FROM = "from";

    /** Estremo superiore, escluso, di una finestra. */
    public static final String TO = "to";

    /** Il profilo di finestra in vigore: {@code CATCH_UP}, {@code REALTIME}, {@code STANDARD}. */
    public static final String WINDOW_PROFILE = "window_profile";

    /** Query verso la sorgente eseguite finora nell'esecuzione. */
    public static final String QUERIES_EXECUTED = "queries_executed";

    /** Righe elaborate finora nell'esecuzione. */
    public static final String ROWS_PROCESSED = "rows_processed";

    /** Righe scritte a destinazione. */
    public static final String ROWS_INSERTED = "rows_inserted";

    /** Righe finite in staging invece che a destinazione. */
    public static final String ROWS_STAGED = "rows_staged";

    /** Il timestamp di checkpoint che l'operazione ha reso persistente. */
    public static final String CHECKPOINT_TS = "checkpoint_ts";

    private LogFields() {
    }
}
