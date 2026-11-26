package it.pagopa.cruscotto.ingestion.fdr.persistence;

/**
 * Il diritto di scrivere il dato business di una giornata, verificato <strong>dentro</strong> la
 * transazione che lo scrive.
 *
 * <h2>Perché non basta controllare prima</h2>
 *
 * <p>La forma ingenua — leggo il token, poi scrivo il batch — lascia aperta una finestra:
 *
 * <pre>
 *   1. il pod A verifica di possedere la giornata        → sì
 *   2. il lease di A scade (A è lento, o la rete non va)
 *   3. il pod B subentra e comincia a riscrivere la giornata
 *   4. A committa comunque il suo batch
 * </pre>
 *
 * <p>Il risultato è una giornata scritta da due proprietari, cioè righe duplicate che nessun
 * contatore segnala: la scrittura FDR è un INSERT fedele, senza deduplica né upsert.
 *
 * <p>La chiusura non è un controllo più frequente, è un controllo <em>nella stessa transazione</em>.
 * {@link #requireOwnership()} esegue un UPDATE condizionato al token sulle due righe
 * {@code FDR_INGESTION_STATUS} della giornata: l'UPDATE prende i lock di quelle righe e li tiene
 * fino al commit, quindi un subentro concorrente resta in attesa invece di sovrapporsi, e se il
 * subentro è già avvenuto l'UPDATE tocca zero righe e la scrittura non parte.
 *
 * <h2>Perché vive qui e non nel package workflow</h2>
 *
 * <p>A chiamarlo è {@link FdrWriteSession}, che sta in {@code persistence}: se l'interfaccia stesse
 * in {@code workflow} la dipendenza si invertirebbe. L'implementazione invece è là, dove si
 * conoscono token e lease — questo file descrive solo il contratto.
 */
@FunctionalInterface
public interface FdrDayFence {

    /**
     * Conferma e rinnova il possesso della giornata, o solleva.
     *
     * <p>Va invocata come <strong>prima</strong> istruzione della transazione che scrive, non
     * altrove: chiamata fuori, i lock che acquisisce vengono rilasciati al ritorno e la garanzia
     * decade silenziosamente.
     *
     * @throws FdrLeaseLostException se la giornata non è più di questo worker. Chi la riceve deve
     *                               interrompersi senza scrivere e senza toccare lo stato della
     *                               giornata, che ormai appartiene a un altro
     */
    void requireOwnership();

    /**
     * Nessuna verifica: per i percorsi che non possiedono una giornata.
     *
     * <p>Esiste per i test del solo livello di scrittura e per il preflight, che legge i file senza
     * scrivere nulla. <strong>Non va usata</strong> su un percorso che scrive dato business: lì
     * l'assenza di chiusura è esattamente il difetto che questa interfaccia esiste per impedire.
     */
    FdrDayFence NONE = () -> {
        // volutamente vuota: vedi il javadoc
    };
}
