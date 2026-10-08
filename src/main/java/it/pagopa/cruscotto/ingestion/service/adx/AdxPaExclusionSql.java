package it.pagopa.cruscotto.ingestion.service.adx;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Clausola KQL che esclude dall'ingestion gli enti creditori indicati in configurazione.
 *
 * <p>Il filtro sta <strong>su ADX e non a valle</strong>: le righe escluse non attraversano la rete,
 * non passano dal transformer e non arrivano a PostgreSQL. E' l'unico punto in cui l'esclusione
 * riduce davvero il volume invece di spostare il costo.</p>
 *
 * <p><strong>Va applicata a tutte e cinque le entita', non solo a POSITION.</strong> Le FK sono
 * obbligatorie: senza la posizione, un token non viene scritto e finisce in
 * {@code STG_INGEST_ERROR} come {@code MISSING_FOREIGN_KEY}, e dietro di lui transfer ed extra info.
 * Escludere a meta' trasformerebbe un risparmio in un accumulo di scarti.</p>
 *
 * <p><strong>Righe senza PA: vengono tenute.</strong> {@code tostring()} di un valore nullo e' la
 * stringa vuota, che non appartiene mai alla lista, quindi passano. E' voluto: su EVENTS_WF un
 * evento puo' essere identificato dal solo TOKEN, e scartarlo perche' privo di PA perderebbe dati
 * buoni. Per lo stesso motivo il confronto e' {@code !in} e non un {@code ==} negato.</p>
 *
 * <p><strong>I codici sono validati, non e' difensivita'.</strong> Finiscono in una query per
 * concatenazione — non esistono bind parameter in KQL — quindi un valore arbitrario sarebbe
 * iniezione. Si accettano solo cifre: e' il formato del codice fiscale di un ente creditore
 * (11 cifre), gia' imposto in scrittura dalla registrazione in anagrafica. Un valore non conforme
 * viene scartato con un WARN invece di essere silenziosamente ignorato o, peggio, interpolato.</p>
 */
@Slf4j
public final class AdxPaExclusionSql {

    /** Solo cifre: qualunque altro carattere renderebbe interpolabile la query. */
    private static final Pattern SAFE_PA_CODE = Pattern.compile("^[0-9]{1,35}$");

    private AdxPaExclusionSql() {
    }

    /**
     * @param excluded codici degli enti da escludere; {@code null} o vuota disattiva il filtro
     * @return il frammento {@code | where ...} da inserire nel template, stringa vuota se non si filtra
     */
    public static String clause(List<String> excluded) {
        if (excluded == null || excluded.isEmpty()) {
            return "";
        }
        String values = excluded.stream()
                .filter(code -> code != null && !code.isBlank())
                .map(String::trim)
                .filter(AdxPaExclusionSql::isSafe)
                .distinct()
                .map(code -> "\"" + code + "\"")
                .collect(Collectors.joining(", "));
        if (values.isEmpty()) {
            return "";
        }
        // trim+tostring come nel resto dei template: il valore sorgente puo' arrivare con spazi.
        return "\n| where trim(\" \", tostring(PA_EMITTENTE)) !in (" + values + ")";
    }

    private static boolean isSafe(String code) {
        if (SAFE_PA_CODE.matcher(code).matches()) {
            return true;
        }
        log.warn("phase=PA_EXCLUSION_REJECTED reason=not-a-numeric-code value={}", code);
        return false;
    }
}
