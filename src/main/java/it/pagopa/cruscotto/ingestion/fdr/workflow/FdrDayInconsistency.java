package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Cosa si fa di una giornata le cui due righe non concordano.
 *
 * <h2>Perché la decisione sta qui e non nel claim</h2>
 *
 * <p>Il claim deve <strong>rifiutarsi e basta</strong>: annulla la transazione e non tocca una
 * riga, così gli status restano la fotografia esatta dell'anomalia e chi va a guardarli vede
 * {@code FLUSSI=1, IUV=4}, non un valore riallineato da noi. Decidere cosa farne è un passo
 * successivo, fuori da quella transazione.
 *
 * <p>Che quel passo esista è però necessario: lasciare la giornata com'è significa che il poller la
 * ritrova ogni cinque minuti, ritenta il claim, fallisce allo stesso modo e scrive un evento. Per
 * sempre. La chiusura terminale la toglie dalla coda e la consegna a una decisione umana — che per
 * una giornata FDR è una REINGESTION.
 *
 * <h2>Perché è una classe di utilità e non un bean</h2>
 *
 * <p>Non ha stato e i tre servizi che la usano hanno già i due repository che le servono. Farne un
 * bean aggiungerebbe un collaboratore a tre costruttori per spostare quattro righe, e i tre
 * servizi FDR hanno già due costruttori a testa — con quello che è costato scoprirlo.
 */
@Slf4j
final class FdrDayInconsistency {

    private FdrDayInconsistency() {
    }

    /**
     * Registra l'anomalia e chiude la giornata, senza rimetterla in coda.
     *
     * <p>L'ordine è evento prima, stato poi: se la seconda scrittura fallisse resterebbe comunque
     * la traccia di cosa è stato rilevato, mentre il contrario lascerebbe una giornata chiusa con
     * un codice che nessun evento spiega.
     *
     * @return quante righe di stato sono state chiuse — attese due, e un valore diverso è a sua
     *         volta degno di nota
     */
    static int settle(FdrIngestionEventRepository eventRepository,
                      FdrIngestionStatusRepository statusRepository,
                      long batchId, LocalDate businessDay, FdrInconsistentDayException failure,
                      Instant now) {
        eventRepository.record(batchId, null, businessDay, null, null,
                FdrEventType.TECHNICAL_ERROR, FdrEventType.ERROR, FdrInconsistentDayException.CODE,
                "The two datasets of the day disagree on the attempt count",
                failure.getMessage(), now);

        int settled = statusRepository.settleInconsistentDay(batchId, businessDay,
                FdrInconsistentDayException.CODE, failure.getMessage(), now);

        try (MdcScope ignored = MdcScope.phase("FDR_CLAIM")
                .with(LogFields.EVENT_OUTCOME, "failure");
             LogDetails details = LogDetails.open()
                     .with("batch_id", batchId)
                     .with("business_day", businessDay.toString())
                     .with(LogFields.REASON, FdrInconsistentDayException.CODE)
                     .with("settled_rows", settled)
                     .with(LogFields.ERROR_DETAIL, failure.getMessage())) {
            // ERROR e non WARN: non si risolve da solo e non si ritenta. Richiede che qualcuno
            // guardi perche' quelle due righe siano state scritte fuori dal claim.
            log.error("Closed an FDR business day whose datasets disagree on the attempt count");
        }
        return settled;
    }
}
