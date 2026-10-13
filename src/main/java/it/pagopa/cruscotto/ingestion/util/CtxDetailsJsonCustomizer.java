package it.pagopa.cruscotto.ingestion.util;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.springframework.boot.json.JsonWriter;
import org.springframework.boot.logging.structured.StructuredLoggingJsonMembersCustomizer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Raccoglie le chiavi volatili dell'MDC nell'oggetto annidato {@code ctx_details}.
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
 * @see LogFields la classificazione fra primo livello e {@code ctx_details}
 */
public class CtxDetailsJsonCustomizer implements StructuredLoggingJsonMembersCustomizer<ILoggingEvent> {

    @Override
    public void customize(JsonWriter.Members<ILoggingEvent> members) {
        members.add(LogFields.CTX_DETAILS, CtxDetailsJsonCustomizer::collectDetails).whenNotEmpty();

        // Rimuove dal primo livello le chiavi appena annidate, altrimenti comparirebbero due
        // volte. Il confronto e' sul path COMPLETO, non sul nome: dentro ctx_details le chiavi si
        // chiamano allo stesso modo (li' il path e' "ctx_details.dependency"), e filtrare per nome
        // cancellerebbe anche quelle, lasciando un ctx_details vuoto.
        members.applyingPathFilter(path -> LogFields.DETAILS.contains(path.toString()));
    }

    private static Map<String, Object> collectDetails(ILoggingEvent event) {
        Map<String, String> mdc = event.getMDCPropertyMap();
        if (mdc == null || mdc.isEmpty()) {
            return Map.of();
        }
        // LinkedHashMap e iterazione sull'ordine dichiarato in LogFields.DETAILS: la riga di log
        // resta confrontabile fra esecuzioni invece di cambiare ordine a ogni avvio.
        Map<String, Object> details = new LinkedHashMap<>();
        for (String key : LogFields.DETAILS) {
            String raw = mdc.get(key);
            if (raw != null && !raw.isBlank()) {
                details.put(key, convert(key, raw));
            }
        }
        return details;
    }

    /**
     * Riporta il valore al tipo dichiarato nel dizionario.
     *
     * <p>Un valore numerico malformato resta <strong>stringa</strong> invece di far saltare la
     * riga: un difetto di formattazione non deve costare la perdita del log, che e' spesso
     * l'unica traccia di cosa stava succedendo.
     */
    private static Object convert(String key, String raw) {
        try {
            if (LogFields.DETAILS_NUMERIC.contains(key)) {
                return Long.valueOf(raw);
            }
            if (LogFields.DETAILS_BOOLEAN.contains(key)) {
                return Boolean.valueOf(raw);
            }
        } catch (NumberFormatException ignored) {
            return raw;
        }
        return raw;
    }
}
