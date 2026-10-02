package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import java.time.temporal.Temporal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Diagnostica di una singola fase, persistita in {@code search_execution_step.metrics} (JSONB).
 *
 * <p>Esiste perche' in produzione non e' possibile lanciare query esplorative: i dati che spiegano
 * <em>perche'</em> un'esecuzione e' lenta, vuota o incompleta vanno registrati dall'ingestor nel
 * momento in cui li osserva. Ogni fase scrive la propria riga in autocommit, quindi le metriche del
 * perimetro sopravvivono a un timeout dei report successivi.</p>
 *
 * <p>Insieme aperto di chiavi, volutamente non tipizzato in colonne: cambiera' man mano che si capisce
 * cosa serve, e ogni aggiunta non deve costare una migration. Chiavi attualmente emesse:</p>
 *
 * <p><strong>Perimetro</strong>: {@code shape} (TOKEN/POSITION/UNION, la forma scelta dai filtri),
 * {@code template}, {@code rows}, {@code chars}, {@code reused}, {@code maxRows}.<br>
 * <strong>Finestra</strong>: {@code from}, {@code to}, {@code source} (periodo utente o lookback di
 * default), {@code defaultLookbackMonths}.<br>
 * <strong>Report</strong>: {@code keys}, {@code batches}, {@code rows}, {@code batchSize},
 * {@code childMarginDays}, {@code slowestBatchMs}, {@code keysPerSec}.<br>
 * <strong>ZIP</strong>: {@code sizeBytes}, {@code reportCount}, {@code totalRows}.</p>
 *
 * <p>Non e' un log: i log si perdono nella rotazione, questa riga resta legata all'esecuzione e si
 * puo' confrontare fra esecuzioni diverse.</p>
 */
public final class StepMetrics {

    private final Map<String, Object> values = new LinkedHashMap<>();

    private StepMetrics() {
    }

    /** @return una nuova diagnostica vuota */
    public static StepMetrics create() {
        return new StepMetrics();
    }

    /**
     * Registra una metrica. I valori {@code null} vengono ignorati, cosi' chi raccoglie non deve
     * preoccuparsi di distinguere "non misurato" da "zero".
     *
     * <p>Le date sono normalizzate a stringa ISO-8601 <em>qui</em>, non lasciate al serializzatore:
     * un {@code ObjectMapper} senza il modulo java-time configurato le scrive come array
     * ({@code [2026,3,1,0,0]}), che nella colonna di diagnostica e' illeggibile e non interrogabile
     * con {@code metrics->>'from'}. In ISO il confronto lessicografico coincide con quello temporale,
     * quindi la colonna resta filtrabile in SQL.</p>
     */
    public StepMetrics with(String key, Object value) {
        if (key != null && value != null) {
            values.put(key, normalize(value));
        }
        return this;
    }

    private Object normalize(Object value) {
        if (value instanceof Temporal || value instanceof Enum<?>) {
            return value.toString();
        }
        return value;
    }

    /** @return {@code true} quando non c'e' nulla da persistere */
    public boolean isEmpty() {
        return values.isEmpty();
    }

    /** @return vista immutabile delle metriche raccolte */
    public Map<String, Object> asMap() {
        return Map.copyOf(values);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
