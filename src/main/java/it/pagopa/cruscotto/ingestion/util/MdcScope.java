package it.pagopa.cruscotto.ingestion.util;

import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Scope di chiavi MDC che al termine <strong>ripristina il valore precedente</strong> invece di
 * cancellarlo.
 *
 * <p>Serve perche' gli scope si annidano. Lo scanner della ricerca massiva imposta
 * {@code entityName} e {@code runId}, poi chiama il servizio di esecuzione che imposta le proprie
 * chiavi e, uscendo, faceva {@code MDC.remove} anche di quelle del chiamante: da quel punto in poi
 * i log dello scanner restavano senza contesto. Con l'encoder ECS quelle chiavi sono attributi di
 * primo livello del JSON, cioe' esattamente i campi su cui si filtra, quindi il difetto e' passato
 * da invisibile a rilevante.</p>
 *
 * <p>La pulizia e' comunque obbligatoria: i thread sono quelli dei pool di Quartz e vengono
 * riusati, quindi una chiave lasciata indietro finirebbe nei log del job successivo attribuendogli
 * un contesto sbagliato — che e' peggio di non averlo.</p>
 *
 * <p>Da usare in try-with-resources:</p>
 * <pre>{@code
 * try (MdcScope ignored = MdcScope.open().with(LogFields.EVENT_ACTION, entity).with(LogFields.CORRELATION_ID, runId)) {
 *     ...
 * }
 * }</pre>
 */
public final class MdcScope implements AutoCloseable {

    /** Valore precedente di ogni chiave toccata; {@code null} significa "non era presente". */
    private final Map<String, String> previous = new LinkedHashMap<>();

    private MdcScope() {
    }

    public static MdcScope open() {
        return new MdcScope();
    }

    /**
     * Scorciatoia per la sola fase, che e' il caso piu' frequente fuori dal percorso di ingestione.
     *
     * <p>Sui servizi che hanno un {@code RunContext} ci pensa {@code LogHelper}; gli altri —
     * ricerca massiva, job di manutenzione — devono impostarla a mano, e scriverne la versione
     * locale in ogni file e' il modo in cui {@code ctx_phase} finisce scritta in quattro modi
     * diversi.
     */
    public static MdcScope phase(String phase) {
        return open().with(LogFields.CTX_PHASE, phase);
    }

    /**
     * Imposta una chiave, memorizzando quello che c'era prima.
     *
     * @param value se {@code null} la chiave viene tolta per la durata dello scope, non lasciata
     *              al valore di un contesto estraneo
     */
    public MdcScope with(String key, String value) {
        if (key == null) {
            return this;
        }
        // Solo la prima volta: con due with() sulla stessa chiave il valore da ripristinare resta
        // quello di partenza, non quello intermedio.
        if (!previous.containsKey(key)) {
            previous.put(key, MDC.get(key));
        }
        if (value == null) {
            MDC.remove(key);
        } else {
            MDC.put(key, value);
        }
        return this;
    }

    @Override
    public void close() {
        previous.forEach((key, value) -> {
            if (value == null) {
                MDC.remove(key);
            } else {
                MDC.put(key, value);
            }
        });
        previous.clear();
    }
}
