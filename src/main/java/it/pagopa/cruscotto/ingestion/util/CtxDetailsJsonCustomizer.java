package it.pagopa.cruscotto.ingestion.util;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.springframework.boot.json.JsonWriter;
import org.springframework.boot.logging.structured.StructuredLoggingJsonMembersCustomizer;

import java.util.Map;
import java.util.TreeMap;

/**
 * Raccoglie le chiavi di dettaglio dell'MDC nell'oggetto annidato {@code ctx_details}.
 *
 * <p><strong>Perche' serve codice e non configurazione.</strong> L'encoder ECS di Spring Boot
 * promuove ogni chiave MDC ad attributo di <em>primo livello</em>, e l'MDC e'
 * {@code String -> String}: non esiste modo di metterci un oggetto. Scriverci una stringa JSON
 * produrrebbe {@code "ctx_details": "{\"dependency\":\"ADX\"}"}, cioe' un campo indicizzato come
 * testo unico — formalmente presente e inutile per filtrare. Questo customizer e' l'unico punto in
 * cui la struttura annidata puo' nascere.
 *
 * <p><strong>Registrazione.</strong> Via {@code logging.structured.json.customizer} in
 * {@code application.yml}. Spring Boot istanzia la classe da se', quindi non e' un bean e non puo'
 * avere dipendenze iniettate: il logging e' attivo molto prima del contesto applicativo.
 *
 * @see LogDetails il punto da cui i dettagli entrano nell'MDC
 */
public class CtxDetailsJsonCustomizer implements StructuredLoggingJsonMembersCustomizer<ILoggingEvent> {

    @Override
    public void customize(JsonWriter.Members<ILoggingEvent> members) {
        members.add(LogFields.CTX_DETAILS, CtxDetailsJsonCustomizer::collectDetails).whenNotEmpty();

        // Rimuove dal primo livello le chiavi appena annidate, altrimenti comparirebbero due
        // volte — una col prefisso tecnico, una dentro ctx_details.
        //
        // Il confronto e' sul path COMPLETO e non sul nome: dentro ctx_details la chiave e' priva
        // di prefisso e il suo path e' "ctx_details.dependency", quindi non combacia e l'oggetto
        // annidato sopravvive. Filtrare per nome svuoterebbe ctx_details, che e' esattamente il
        // difetto in cui sono incorso scrivendo la prima versione.
        members.applyingPathFilter(path -> isDetailKey(path.toString()));
    }

    private static boolean isDetailKey(String path) {
        for (String prefix : LogFields.DETAIL_PREFIXES) {
            if (path.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, Object> collectDetails(ILoggingEvent event) {
        Map<String, String> mdc = event.getMDCPropertyMap();
        if (mdc == null || mdc.isEmpty()) {
            return Map.of();
        }
        // TreeMap: l'ordine delle chiavi nell'MDC non e' garantito, e una riga di log che cambia
        // ordine a ogni avvio non e' confrontabile ne' testabile.
        Map<String, Object> details = new TreeMap<>();
        mdc.forEach((key, raw) -> {
            if (raw == null || raw.isBlank()) {
                return;
            }
            if (key.startsWith(LogFields.DETAIL_NUMERIC_PREFIX)) {
                details.put(strip(key, LogFields.DETAIL_NUMERIC_PREFIX), asNumber(raw));
            } else if (key.startsWith(LogFields.DETAIL_BOOLEAN_PREFIX)) {
                details.put(strip(key, LogFields.DETAIL_BOOLEAN_PREFIX), Boolean.valueOf(raw));
            } else if (key.startsWith(LogFields.DETAIL_TEXT_PREFIX)) {
                details.put(strip(key, LogFields.DETAIL_TEXT_PREFIX), raw);
            }
        });
        return details;
    }

    /** Toglie il prefisso che ha fatto match, senza assumere che i tre abbiano la stessa lunghezza. */
    private static String strip(String key, String prefix) {
        return key.substring(prefix.length());
    }

    /**
     * Un valore numerico malformato resta <strong>stringa</strong> invece di far saltare la riga:
     * un difetto di formattazione non deve costare la perdita del log, che e' spesso l'unica
     * traccia di cosa stava succedendo.
     */
    private static Object asNumber(String raw) {
        try {
            return Long.valueOf(raw);
        } catch (NumberFormatException notALong) {
            try {
                return Double.valueOf(raw);
            } catch (NumberFormatException notADouble) {
                return raw;
            }
        }
    }
}
