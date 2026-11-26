package it.pagopa.cruscotto.ingestion.fdr.config;

import java.util.UUID;

/**
 * Identificativo del processo che sta elaborando: in Kubernetes il nome del pod.
 *
 * <p>Esiste come classe FDR, e non riusando l'equivalente dell'ingestione ADX, perché quello vive
 * su {@code ExecutionLogService} — la classe che scrive {@code INGEST_EXECUTION_LOG}, da cui FDR si
 * è separato. Dipenderne per un identificativo di tre righe avrebbe reso il modulo di nuovo
 * sensibile al ciclo di vita dell'ingestor, e per un motivo puramente cosmetico. Il valore è
 * calcolato allo stesso modo, quindi i due coincidono su uno stesso pod: non è una divergenza.
 *
 * <p><strong>A cosa serve davvero.</strong> In produzione non c'è accesso diretto ai log, quindi lo
 * stato a database deve bastare per la diagnosi. Finché FDR scriveva nell'execution log, quale pod
 * possedesse una giornata era leggibile lì; togliendolo, senza questo valore su
 * {@code FDR_INGESTION_STATUS} una giornata bloccata non sarebbe attribuibile ad alcun processo.
 * Non è ownership — quella è il token — è la risposta a "chi devo andare a guardare".
 */
public final class FdrInstanceId {

    /** Risolto una volta sola: cambia solo riavviando il processo. */
    private static final String VALUE = resolve();

    private FdrInstanceId() {
    }

    public static String get() {
        return VALUE;
    }

    /**
     * {@code HOSTNAME} è il nome del pod in Kubernetes ed è il valore che serve.
     *
     * <p>Il ripiego su un UUID non è decorativo: fuori da Kubernetes — una macchina di sviluppo, un
     * test — la variabile può non esserci, e un valore vuoto in colonna si confonderebbe con "non
     * ancora scritto". Meglio un identificativo che non dice dove, ma distingue i processi.
     */
    private static String resolve() {
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isBlank()) {
            return hostname;
        }
        return "local-" + UUID.randomUUID();
    }
}
