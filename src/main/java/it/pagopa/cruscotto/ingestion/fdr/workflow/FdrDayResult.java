package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.LocalDate;
import java.util.Map;

/**
 * L'esito di una giornata FDR intera.
 *
 * <p>La giornata è l'unità minima: i due dataset si elaborano insieme e si leggono insieme. Lo
 * stato complessivo non è una media né il peggiore in senso lato, ma segue la regola del §13 —
 * {@link FdrProcessingStatus#WAITING_RETRY} ha la precedenza su tutto, perché un guasto tecnico
 * non dice ancora come sarebbe finita.
 *
 * @param skipped se la giornata non è stata elaborata perché già in uno stato terminale di
 *                successo. Non è un fallimento: è la guardia di idempotenza che ha funzionato, e
 *                va distinto da una giornata appena completata
 */
public record FdrDayResult(
        long batchId,
        LocalDate businessDay,
        FdrProcessingStatus status,
        Map<FdrDataset, FdrDatasetResult> datasets,
        boolean skipped) {

    /** X della giornata: la somma dei due dataset. */
    public long rowsSource() {
        return datasets.values().stream().mapToLong(FdrDatasetResult::rowsSource).sum();
    }

    /** N della giornata. */
    public long rowsProcessed() {
        return datasets.values().stream().mapToLong(FdrDatasetResult::rowsProcessed).sum();
    }
}
