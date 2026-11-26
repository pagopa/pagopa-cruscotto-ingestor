package it.pagopa.cruscotto.ingestion.fdr.workflow;

/**
 * Lo stato di un dataset giornaliero e, con gli stessi valori, della giornata che li aggrega.
 *
 * <p>Gli stessi nomi per i due livelli non sono pigrizia: la giornata <em>è</em> la composizione
 * dei suoi due dataset, e dare due vocabolari a una relazione di contenimento costringerebbe a
 * tradurre fra loro a ogni lettura.
 *
 * <p>Va in {@code FDR_INGESTION_STATUS.STATUS}, che è {@code VARCHAR(32)} <strong>senza CHECK</strong>:
 * aggiungere uno stato non costa una migration, ma neanche il database lo difende — questo enum è
 * l'unico posto in cui l'insieme è chiuso.
 */
public enum FdrProcessingStatus {

    /**
     * Registrata e non ancora presa in carico.
     *
     * <p>È lo stato in cui nascono le righe di una richiesta amministrativa: il backend — o
     * l'espansione di un BOOTSTRAP — dichiara quali giornate vuole, e solo dopo qualcuno le prende.
     *
     * <p><strong>Deve stare in questo enum anche se non lo scrive il core.</strong> La colonna non
     * ha un CHECK, quindi il database accetterebbe qualunque stringa, ma ogni lettura passa da
     * {@code valueOf}: un valore scritto e non dichiarato qui non sarebbe uno stato sconosciuto da
     * ignorare, sarebbe un'eccezione al primo tentativo di rileggere la riga.
     */
    PENDING,

    /** Presa in carico. Uno status fermo qui dopo un riavvio è lavoro interrotto, non perso. */
    PROCESSING,

    /** X = N > 0, nessun warning. */
    COMPLETED,

    /** Elaborato, ma con qualcosa da guardare: righe perse, file vuoti, valori accorciati. */
    COMPLETED_WITH_WARNING,

    /** Terminale e <strong>non</strong> ritentabile: rileggere lo stesso blob darebbe lo stesso esito. */
    FAILED,

    /**
     * La discovery è riuscita e del dataset non c'è alcun file.
     *
     * <p>Distinto da {@link #FAILED} perché la causa è fuori da qui — il file non è stato
     * pubblicato — e distinto da {@link #WAITING_RETRY} perché ritentare fra un'ora non lo fa
     * comparire. È una segnalazione, non un guasto.
     */
    MISSING,

    /**
     * Guasto tecnico transitorio: Azure irraggiungibile, database giù, timeout.
     *
     * <p>Ha <strong>precedenza</strong> su un esito terminale: se la giornata non è stata elaborata
     * per un guasto, non si sa ancora se sarebbe finita COMPLETED o FAILED, e dichiararlo sarebbe
     * un'invenzione.
     */
    WAITING_RETRY;

    /** Se lo stato è definitivo e la giornata non va rielaborata senza una REINGESTION esplicita. */
    public boolean isTerminalSuccess() {
        return this == COMPLETED || this == COMPLETED_WITH_WARNING;
    }

    /**
     * Se il dataset è <strong>concluso</strong>, con qualunque esito: nessuno ci sta lavorando e
     * nessuno lo riprenderà da solo.
     *
     * <p>Serve al claim, che ragiona per <em>giornata</em>: un dataset concluso non impedisce di
     * riprendere il suo gemello, perché non c'è un worker da scavalcare. È una domanda diversa da
     * {@link #isTerminalSuccess()}, che invece decide se {@code FdrDayScope.RESUME} può
     * <em>saltare</em> il dataset.
     *
     * <p>La differenza fra le due conta, ed è misurabile: un dataset terminale <em>non</em> di
     * successo ha scritto <strong>zero</strong> righe business — {@link #FAILED} si raggiunge solo
     * con {@code X = 0} o {@code N = 0}, {@link #MISSING} senza alcun file — quindi rielaborarlo non
     * può duplicare nulla, e su una giornata che si sta comunque ritentando è semmai un'occasione in
     * più. {@link #COMPLETED_WITH_WARNING} invece le righe le ha scritte, ed è per questo che RESUME
     * lo salta.
     */
    public boolean isSettled() {
        return isTerminalSuccess() || this == FAILED || this == MISSING;
    }
}
