package it.pagopa.cruscotto.ingestion.fdr.persistence;

/**
 * Quello che la sessione di scrittura sa di sé mentre sta ancora lavorando.
 *
 * <p>È la differenza fra {@link FdrWriteResult} e questo: il primo è il consuntivo e arriva una
 * volta sola, alla fine; questo è leggibile in qualunque momento e serve a chi deve raccontare un
 * file ancora aperto. Senza, una giornata interrotta a metà risulterebbe con tutti i contatori a
 * zero, e "non ha letto niente" sarebbe indistinguibile da "si è fermato quasi alla fine".
 *
 * <p>Porta <strong>solo</strong> i conteggi, non il campione degli errori né la lista delle
 * troncature: quelle crescono, e uno snapshot che le copiasse a ogni batch rifarebbe il lavoro che
 * il limite di memoria del writer esiste per evitare.
 *
 * @param rowsReceived le righe consegnate allo scrittore finora
 * @param rowsInserted quante sono arrivate in banca dati
 * @param rowsRejected quante il database ha rifiutato
 * @param rowsTruncated quante hanno avuto la {@code CAUSALE} accorciata. Sono righe riuscite, e
 *                      stanno dentro {@code rowsInserted}
 */
public record FdrWriteProgress(long rowsReceived, long rowsInserted, long rowsRejected,
                               long rowsTruncated) {
}
