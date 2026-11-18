package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

/**
 * L'esito di un dataset giornaliero: {@code businessDay + dataset}.
 *
 * <p>X e N sono i due numeri su cui si decide tutto. <strong>X</strong> è quante righe dati il
 * dataset conteneva, sommate sui suoi file; <strong>N</strong> è quante ne sono <em>arrivate in
 * banca dati</em>. Non è la stessa cosa delle righe mappate dal lettore: una riga che il database
 * rifiuta è stata letta benissimo e non c'è, quindi N viene dal writer.
 *
 * @param rowsSource    X
 * @param rowsProcessed N
 * @param rowsFailed    X − N, tenuto esplicito perché è quello che va in colonna e che si confronta
 *                      fra una giornata e l'altra
 * @param errorCode     breve, destinato a {@code LAST_ERROR_CODE}; {@code null} quando non c'è
 *                      niente da dire
 */
public record FdrDatasetResult(
        FdrDataset dataset,
        FdrProcessingStatus status,
        long rowsSource,
        long rowsProcessed,
        long rowsFailed,
        String errorCode,
        String errorMessage) {

    static FdrDatasetResult of(FdrDataset dataset, FdrProcessingStatus status, long x, long n,
                               String errorCode, String errorMessage) {
        return new FdrDatasetResult(dataset, status, x, n, Math.max(x - n, 0), errorCode,
                errorMessage);
    }
}
