package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

/**
 * Il possesso di una giornata FDR da parte di un worker, per un tempo limitato.
 *
 * <h2>L'unità è la giornata, non il dataset</h2>
 *
 * <p>Una giornata è sempre <strong>due</strong> righe di {@code FDR_INGESTION_STATUS} —
 * {@code FLUSSI_RENDICONTAZIONE} e {@code IUV_RENDICONTATI} — e il cliente ha stabilito che si
 * elaborano e si reingeriscono insieme. Il lease riflette quella decisione: un solo token, una sola
 * scadenza, un solo numero di tentativo per entrambe. Non esiste un lease su mezza giornata.
 *
 * <p>La conseguenza pratica di non averlo fatto così sarebbe grave proprio dove conta: la
 * reingestione svuota le due partizioni in un colpo solo, e un pod che possedesse i soli IUV
 * cancellerebbe anche i FLUSSI di un pod diverso.
 *
 * @param batchId       la richiesta a cui la giornata appartiene
 * @param businessDay   la giornata FDR, cioè la cartella {@code yyyyMMdd} del blob
 * @param token         identificativo opaco del possesso. Cambia a ogni claim, anche sullo stesso
 *                      pod: è ciò che distingue questo tentativo dal precedente, e che rende
 *                      inefficaci le scritture di un worker subentrato
 * @param attemptNo     il tentativo corrente, identico sulle due righe. Il primo è 1, mai 0
 * @param statusIds     l'identificativo di riga per dataset, usato dal log per file
 * @param previousStatus lo stato di ciascun dataset <strong>prima</strong> di questo claim. Il
 *                      claim lo sovrascrive con {@code PROCESSING}, quindi se non venisse
 *                      catturato qui sarebbe perso: serve alla ripresa, che deve saper distinguere
 *                      un dataset gia' concluso con successo da uno da rifare. Senza, un retry di
 *                      una giornata in cui solo gli IUV erano falliti rielaborerebbe anche i FLUSSI
 *                      e ne <em>raddoppierebbe</em> le righe
 * @param leaseExpiresAt fino a quando il possesso vale, se non viene rinnovato
 */
public record FdrDayLease(long batchId,
                          LocalDate businessDay,
                          String token,
                          int attemptNo,
                          Map<FdrDataset, Long> statusIds,
                          Map<FdrDataset, FdrProcessingStatus> previousStatus,
                          Instant leaseExpiresAt) {

    public FdrDayLease {
        statusIds = Map.copyOf(statusIds);
        previousStatus = Map.copyOf(previousStatus);
    }

    /**
     * Se il dataset era gia' concluso con successo prima di questo tentativo.
     *
     * <p>Chi riprende una giornata deve saltarlo: la scrittura FDR e' un INSERT fedele, senza
     * deduplica, quindi rielaborare un dataset gia' scritto non lo aggiorna — lo duplica.
     */
    public boolean wasAlreadyDone(FdrDataset dataset) {
        FdrProcessingStatus status = previousStatus.get(dataset);
        return status != null && status.isTerminalSuccess();
    }

    /** L'identificativo di riga del dataset, per il log per file. */
    public Long statusIdOf(FdrDataset dataset) {
        return statusIds.get(dataset);
    }
}
