package it.pagopa.cruscotto.ingestion.fdr.workflow;

/**
 * I tipi di evento operativo che il core di giornata scrive in {@code FDR_INGESTION_EVENT}.
 *
 * <p>Costanti e non un enum perché la colonna è {@code VARCHAR(64)} e il registro è destinato a
 * crescere con i workflow che verranno; un enum obbligherebbe a toccare questo file per ogni
 * evento nuovo di un altro strato. Qui stanno quelli che il core produce davvero.
 *
 * <p><strong>Tutti aggregati.</strong> Nessuno di questi viene scritto per riga: l'evento più
 * sensibile, {@link #ROWS_DISCARDED}, riassume in una riga tutte le righe scartate di un file
 * riga sola. La tabella eventi serve a leggere una giornata, non a copiarla.
 */
public final class FdrEventType {

    /** Entry del blob non elaborabili, riassunte per ragione. Una per giornata. */
    public static final String DISCOVERY_ANOMALIES = "DISCOVERY_ANOMALIES";

    /** File valido con header e zero righe. Non è un fallimento: vedi la regola X/N. */
    public static final String EMPTY_SOURCE_FILE = "EMPTY_SOURCE_FILE";

    /** File presenti ma zero record in tutto il dataset. Terminale e non ritentabile. */
    public static final String EMPTY_DATASET = "EMPTY_DATASET";

    /** Nessun file del dataset dopo una discovery riuscita. */
    public static final String DATASET_MISSING = "DATASET_MISSING";

    /** Il troncamento autorizzato di CAUSALE, aggregato per dataset e giornata. */

    /** Un file illeggibile: archivio corrotto, codifica non valida, header incoerente. */
    public static final String FILE_STRUCTURAL_ERROR = "FILE_STRUCTURAL_ERROR";

    /** Righe scartate in lettura o rifiutate dal database, riassunte per file. */
    public static final String ROWS_DISCARDED = "ROWS_DISCARDED";

    /** Guasto tecnico che ha impedito l'elaborazione: Azure, database, trasporto. */
    public static final String TECHNICAL_ERROR = "TECHNICAL_ERROR";

    /** Un altro tentativo è stato programmato. Uno per tentativo, non uno per file. */
    public static final String RETRY_SCHEDULED = "RETRY_SCHEDULED";

    /** I tentativi sono finiti: l'esito diventa terminale. */
    public static final String RETRY_EXHAUSTED = "RETRY_EXHAUSTED";

    /** Severità ammesse dalla colonna, nei soli tre valori che il core usa. */
    public static final String INFO = "INFO";
    public static final String WARNING = "WARNING";
    public static final String ERROR = "ERROR";

    private FdrEventType() {
    }
}
