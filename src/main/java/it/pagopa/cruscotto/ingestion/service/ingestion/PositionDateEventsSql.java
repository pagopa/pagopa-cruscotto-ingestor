package it.pagopa.cruscotto.ingestion.service.ingestion;

/**
 * Unico punto in cui e' definito l'aggiornamento di {@code POSITION.LAST_EVENT} e
 * {@code POSITION.DATE_EVENTS} a fronte di un evento che si aggancia a una POSITION gia' esistente.
 *
 * <p>DATE_EVENT e INSERTED_TIMESTAMP sono le coordinate di NASCITA della posizione e non compaiono
 * qui: non vanno mai riscritte da un evento successivo, perche' sono l'ancora sia della finestra di
 * merge a 24h sia della risoluzione FK dei figli.</p>
 *
 * <p>Hanno due scrittori concorrenti ({@code BulkWriterImpl} per gli eventi POSITION,
 * {@code PositionEventUpdateService} per gli EVENTS_WF) che girano in job Quartz distinti, quindi in
 * parallelo. L'aggiornamento e' percio' espresso come singola UPDATE atomica, senza
 * read-modify-write applicativo: leggere l'array in Java e riscriverlo per intero perderebbe i
 * giorni aggiunti dall'altro job nel frattempo.</p>
 *
 * <p>La UPDATE preserva la semantica storica dell'array: unione senza duplicati, ordinamento
 * crescente, esclusione del giorno di nascita e normalizzazione al formato ISO {@code yyyy-MM-dd}
 * richiesto dal requisito. I valori gia' a DB nel vecchio formato {@code yyyyMMdd} vengono
 * riconosciuti e convertiti, quindi le righe legacy si sanano da sole al primo tocco.</p>
 *
 * <p>La conversione e' fatta con sole operazioni su stringa, senza alcun cast a {@code date}:
 * un cast fallisce a runtime su valori che hanno la forma giusta ma non sono date valide
 * (es. {@code "20261340"}), e in un batch UPDATE l'errore non colpisce solo la riga sporca ma
 * aborta l'intera transazione. Gli elementi che non superano la validazione vengono scartati.</p>
 */
public final class PositionDateEventsSql {

    /** Data ISO con mese 01-12 e giorno 01-31. */
    private static final String ISO_PATTERN = "^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$";

    /** Stesso vincolo, formato compatto yyyyMMdd scritto storicamente da PositionEventUpdateService. */
    private static final String BASIC_ISO_PATTERN = "^[0-9]{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$";

    private PositionDateEventsSql() {
    }

    /**
     * UPDATE con 4 bind: 1 = LAST_EVENT candidato (timestamp, nullable), 2 e 3 = giorno dell'evento
     * in ISO (text, nullable), 4 = ID della POSITION.
     */
    public static String appendDateEvent(String schema) {
        // Difesa contro un DATE_EVENTS non-array: jsonb_array_elements_text fallirebbe a runtime.
        String currentArray = "CASE WHEN jsonb_typeof(DATE_EVENTS) = 'array' THEN DATE_EVENTS ELSE '[]'::jsonb END";
        return "UPDATE " + schema + ".POSITION SET " +
                // GREATEST ignora i NULL: LAST_EVENT non regredisce se un evento viene riprocessato
                // fuori ordine (reconciliation, retry da staging).
                "LAST_EVENT = GREATEST(LAST_EVENT, ?::timestamp), " +
                "DATE_EVENTS = CASE WHEN ?::text IS NULL THEN " + currentArray + " ELSE COALESCE((" +
                "SELECT jsonb_agg(DISTINCT d ORDER BY d) FROM (" +
                "SELECT CASE " +
                "WHEN x ~ '" + ISO_PATTERN + "' THEN x " +
                "WHEN x ~ '" + BASIC_ISO_PATTERN + "' " +
                "THEN substr(x, 1, 4) || '-' || substr(x, 5, 2) || '-' || substr(x, 7, 2) " +
                "END AS d " +
                "FROM jsonb_array_elements_text(" + currentArray + " || to_jsonb(?::text)) AS t(x)" +
                ") s WHERE d IS NOT NULL AND d <> to_char(DATE_EVENT, 'YYYY-MM-DD')" +
                "), '[]'::jsonb) END " +
                "WHERE ID = ?";
    }
}
