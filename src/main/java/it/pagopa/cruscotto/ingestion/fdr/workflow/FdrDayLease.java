package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.Instant;
import java.time.LocalDate;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

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

    /**
     * Dichiara come gia' conclusi dei dataset che sono stati scritti da un'<strong>altra</strong>
     * richiesta.
     *
     * <h3>Il difetto che chiude</h3>
     *
     * <p>{@code previousStatus} nasce dalle righe di <em>questo</em> batch, e ogni richiesta ha le
     * proprie: un BOOTSTRAP che copre una giornata gia' ingerita dal DAILY si crea righe nuove in
     * {@code PENDING}, le reclama, e {@code FdrDayScope.RESUME} non salta nulla perche' per lui
     * quella giornata non e' mai stata fatta. La scrittura e' un INSERT fedele, quindi il risultato
     * non e' un aggiornamento ma un <strong>raddoppio</strong> — e nessun contatore lo segnala,
     * perche' X ed N descrivono questa elaborazione e non la tabella.
     *
     * <p>Sovrapponendo qui cio' che risulta gia' presente, la decisione torna dove gia' esiste:
     * RESUME salta esattamente i dataset da saltare e {@code carryOver} ne riporta l'esito, senza
     * un secondo meccanismo parallelo da tenere allineato.
     *
     * <h3>Perche' per dataset e non per giornata</h3>
     *
     * <p>Una giornata puo' essere <em>parzialmente</em> presente: i due dataset si elaborano in
     * sequenza, quindi un guasto a meta' lascia FLUSSI concluso e IUV no. Saltare l'intera giornata
     * lascerebbe gli IUV mancanti per sempre; non saltare nulla duplicherebbe i FLUSSI. Solo la
     * granularita' del dataset fa la cosa giusta in entrambi i casi.
     *
     * <p>Non declassa mai: un dataset che in questo batch risulta gia' terminale positivo resta
     * com'e'. La sovrapposizione puo' solo aggiungere.
     *
     * @param alreadyIngested i dataset gia' scritti da un'altra richiesta per questa giornata
     * @return lo stesso lease se non c'e' niente da sovrapporre, altrimenti una copia aggiornata
     */
    public FdrDayLease withAlreadyIngested(Set<FdrDataset> alreadyIngested) {
        if (alreadyIngested == null || alreadyIngested.isEmpty()) {
            return this;
        }
        Map<FdrDataset, FdrProcessingStatus> merged = new EnumMap<>(previousStatus);
        for (FdrDataset dataset : alreadyIngested) {
            merged.merge(dataset, FdrProcessingStatus.COMPLETED,
                    (corrente, gia) -> corrente.isTerminalSuccess() ? corrente : gia);
        }
        return new FdrDayLease(batchId, businessDay, token, attemptNo, statusIds, merged,
                leaseExpiresAt);
    }
}
