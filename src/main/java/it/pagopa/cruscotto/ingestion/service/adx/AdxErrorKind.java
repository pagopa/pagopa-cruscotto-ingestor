package it.pagopa.cruscotto.ingestion.service.adx;

import java.util.List;
import java.util.Locale;

/**
 * Classificazione stabile e <strong>interrogabile</strong> di un errore ADX.
 *
 * <p>Nasce da un difetto concreto: {@code ERROR_CODE} nel log di esecuzione vale
 * {@code e.getClass().getSimpleName()}, quindi per ogni fallimento ADX contiene sempre
 * {@code AdxQueryFailedException}. Il discriminante vero — throttling, rowstore, risultato troppo
 * grande, timeout — resta sepolto nel testo libero di {@code ERROR_MESSAGE}. Durante l'incidente del
 * 6-7 ottobre 2026 questo ha prodotto un'analisi <em>sbagliata</em>: raggruppando per
 * {@code ERROR_CODE} si vedeva un'unica categoria, e dieci rifiuti per throttling sono stati
 * scoperti solo rileggendo i messaggi a mano.</p>
 *
 * <p><strong>L'ordine di valutazione e' la parte delicata.</strong> Il messaggio reale del rowstore
 * contiene <em>anche</em> le stringhe {@code "Partial query failure"} e
 * {@code "Internal service error"}: senza una precedenza esplicita finirebbe nella categoria
 * sbagliata, cioe' quella generica. Le regole vanno quindi dalla piu' specifica alla piu' generica,
 * e l'ordine e' bloccato da test.</p>
 *
 * <p>I pattern sono le <strong>stringhe realmente osservate in produzione</strong>, non quelle
 * supposte. Esempio: il throttling si presenta come
 * {@code "ThrottleException: Request was throttled, too many requests."} — con le parole separate,
 * quindi {@code "TooManyRequests"} non lo intercetta.</p>
 *
 * <p>Questa classe <strong>non decide il comportamento</strong> (retry, dimezzamento della
 * finestra): serve solo a rendere l'errore interrogabile. Allinearci sopra anche le decisioni e' un
 * lavoro successivo e deliberatamente separato, perche' cambia cosa fa l'ingestor.</p>
 */
public enum AdxErrorKind {

    /** Rifiutati all'ammissione: su ADX il limite e' di cluster, non per chiamante. */
    THROTTLED(List.of("throttleexception", "too many requests", "e_too_many_requests",
        "aborted due to throttling")),

    /** Il motore non riesce a leggere dal rowstore. Va PRIMA dei due generici che contiene. */
    ROWSTORE(List.of("e_rs_cannot_retrieve_values", "retrieve values from rowstore")),

    /** Finestra troppo ampia per i limiti di servizio: si cura dimezzandola, non ritentandola. */
    RESULT_TOO_LARGE(List.of("e_query_result_set_too_large", "limitsexceeded",
        "exceeded the allowed limits", "e_runaway_query")),

    LOW_MEMORY(List.of("e_low_memory_condition", "memory budget")),

    TIMEOUT(List.of("read timed out", "timed out in post request", "connect timed out",
        "sockettimeoutexception", "requestexecutiontimeout")),

    NETWORK(List.of("connection reset", "connection refused", "socketexception",
        "unknownhostexception")),

    /** Generico: arriva qui solo se nessuna causa piu' precisa ha gia' preso il messaggio. */
    PARTIAL_QUERY_FAILURE(List.of("partial query failure", "partialqueryfailure")),

    /** Generico lato servizio, tipicamente con failureCode 5xx. */
    INTERNAL_SERVICE_ERROR(List.of("internal service error")),

    UNKNOWN(List.of());

    private final List<String> patterns;

    AdxErrorKind(List<String> patterns) {
        this.patterns = patterns;
    }

    /**
     * @param error messaggio d'errore gia' appiattito (vedi {@code AdxClientImpl#buildErrorMessage})
     * @return la prima categoria che riconosce il messaggio, {@link #UNKNOWN} se nessuna
     */
    public static AdxErrorKind classify(String error) {
        if (error == null || error.isBlank()) {
            return UNKNOWN;
        }
        String lower = error.toLowerCase(Locale.ROOT);
        for (AdxErrorKind kind : values()) {
            if (kind.patterns.stream().anyMatch(lower::contains)) {
                return kind;
            }
        }
        return UNKNOWN;
    }

    /**
     * Codice da scrivere in {@code ERROR_CODE}: il nome dell'eccezione resta come prefisso per non
     * invalidare ricerche e alert esistenti, la categoria lo rende raggruppabile.
     *
     * @return {@code NomeEccezione} oppure {@code NomeEccezione/CATEGORIA}
     */
    public static String errorCode(String exceptionName, String error) {
        AdxErrorKind kind = classify(error);
        return kind == UNKNOWN ? exceptionName : exceptionName + "/" + kind.name();
    }
}
