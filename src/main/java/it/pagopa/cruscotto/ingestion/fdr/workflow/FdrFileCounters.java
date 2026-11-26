package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReadResult;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteProgress;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteResult;

/**
 * I cinque numeri di un file, e la distinzione fra due di essi che è il punto di tutta la classe.
 *
 * <p><strong>{@code rowsSource} non è {@code rowsRead}.</strong> Il primo è <em>quante righe aveva
 * il file</em>, e si conosce solo dopo averlo letto fino in fondo; il secondo è <em>quante ne sono
 * state effettivamente viste</em>. Su un file completato coincidono, ed è per questo che la
 * distinzione sembra superflua — fino al file che si rompe a metà, dove valgono cose opposte:
 * {@code rowsRead} dice dove ci si è fermati, {@code rowsSource} resta <strong>nullo</strong>
 * perché il totale non lo sa nessuno. Scriverci il parziale sarebbe l'errore peggiore possibile:
 * un file da tre milioni di righe interrotto a quarantamila risulterebbe un file da quarantamila
 * righe completato quasi per intero.
 *
 * @param rowsSource     X: le righe dati del file, header escluso. {@code null} finché non si è
 *                       raggiunta la fine del file
 * @param rowsRead       le righe dati effettivamente incontrate, comprese quelle che poi sono
 *                       fallite. Su un file interrotto è il punto a cui si è arrivati
 * @param rowsProcessed  N: le righe arrivate in banca dati. Una riga accettata dal lettore e
 *                       rifiutata dal database non conta qui
 * @param rowsFailed     le righe lette e non arrivate in banca dati. <strong>Contatore autonomo</strong>,
 *                       non {@code rowsSource − rowsProcessed}: quella differenza non è calcolabile
 *                       quando {@code rowsSource} è nullo, cioè proprio quando serve
 * @param rowsTruncated  le righe la cui {@code CAUSALE} è stata accorciata. Sono righe
 *                       <strong>riuscite</strong>: contano in {@code rowsProcessed} e non in
 *                       {@code rowsFailed}
 */
public record FdrFileCounters(Long rowsSource, long rowsRead, long rowsProcessed, long rowsFailed,
                              long rowsTruncated) {

    /** Lo stato all'apertura della riga: nulla di noto, e nessun totale da dichiarare. */
    public static FdrFileCounters empty() {
        return new FdrFileCounters(null, 0, 0, 0, 0);
    }

    /**
     * Lo stato a metà lavoro, dal solo scrittore.
     *
     * <p>A metà di un file il lettore non ha ancora comunicato nulla: l'unica vista disponibile è
     * quella dello scrittore, che conosce le righe ricevute, inserite e rifiutate. È una vista
     * <strong>coerente ma parziale</strong> — non vede le righe che il lettore ha scartato prima di
     * passargliele — e la coerenza è ciò che conta: vale {@code rowsRead = rowsProcessed +
     * rowsFailed} anche qui, come a file chiuso.
     *
     * <p>{@code rowsSource} resta nullo per costruzione: a metà lettura il totale non esiste.
     */
    public static FdrFileCounters inProgress(FdrWriteProgress progress) {
        return new FdrFileCounters(null, progress.rowsReceived(), progress.rowsInserted(),
                progress.rowsRejected(), progress.rowsTruncated());
    }

    /**
     * Lo stato a file letto fino in fondo.
     *
     * <p>Qui, e solo qui, {@code rowsSource} viene valorizzato: il lettore ha raggiunto la fine e
     * il totale è un fatto. {@code rowsFailed} somma le due famiglie di scarto — quelle del lettore
     * (riga illeggibile o non mappabile) e quelle dello scrittore (riga rifiutata dal database) —
     * che sono eventi diversi con la stessa conseguenza: la riga non c'è.
     */
    public static FdrFileCounters completed(FdrFileReadResult read, FdrWriteResult written) {
        return new FdrFileCounters(read.dataRows(), read.dataRows(), written.rowsInserted(),
                read.rejectedRows() + written.rowsRejected(),
                written.truncation().affectedRows());
    }

    /**
     * Lo stato a lettura interrotta: tutto ciò che si sa, e nessun totale.
     *
     * <p>Lo scrittore è l'unica fonte sopravvissuta — il lettore è morto a metà senza restituire
     * nulla — quindi i conteggi sono i suoi.
     */
    public static FdrFileCounters interrupted(FdrWriteProgress progress) {
        return inProgress(progress);
    }

    /**
     * Se qualcosa è stato <em>perso</em>: è ciò che distingue COMPLETED da WITH_WARNING.
     *
     * <p>{@code rowsTruncated} è deliberatamente fuori. L'accorciamento della {@code CAUSALE} a 30
     * caratteri è la trasformazione che il cliente ha chiesto sul proprio dato, non un'anomalia, e
     * su una giornata reale tocca una riga su cinque: lasciarlo qui marcava con riserva ogni file
     * FLUSSI di ogni giornata, cioè rendeva lo stato inutile proprio quando serve a distinguere.
     * Il conteggio resta in colonna, come contatore.
     *
     * <p>Deve restare allineato a {@code DatasetTotals.hasWarnings()}: se i due divergessero, un
     * dataset {@code COMPLETED} conterrebbe file {@code COMPLETED_WITH_WARNING} e chi legge non
     * saprebbe a quale dei due credere.
     */
    public boolean hasWarnings() {
        return rowsFailed > 0;
    }
}
