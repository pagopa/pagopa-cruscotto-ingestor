/**
 * Il core che elabora una giornata FDR, comune a tutti i workflow.
 *
 * <p>Qui vive {@code processDay}: discovery, lettura, scrittura, esito. DAILY, BOOTSTRAP,
 * REINGESTION e il retry useranno tutti questo stesso percorso, e per questo il core non sa nulla
 * di loro — non consulta l'orologio per scegliere la giornata, non cicla su più giornate, non
 * tronca nulla.
 *
 * <p>I tre livelli da non confondere, perché solo quello di mezzo ha uno stato persistito:
 * <ul>
 *   <li><strong>file fisico</strong>: un {@code .csv.gz}. Produce conteggi, warning ed eventi, non
 *       uno stato suo;</li>
 *   <li><strong>dataset giornaliero</strong>: {@code businessDay + dataset}. È la riga di
 *       {@code FDR_INGESTION_STATUS};</li>
 *   <li><strong>giornata FDR</strong>: i due dataset insieme. È l'unità minima di elaborazione, e
 *       di reingestion.</li>
 * </ul>
 *
 * <p>Non c'è ancora nessuno scheduler, nessun polling e nessun TRUNCATE: quelli arrivano dopo, e
 * tutti e tre chiameranno questo.
 */
package it.pagopa.cruscotto.ingestion.fdr.workflow;
