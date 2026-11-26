package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrDayFence;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrLeaseLostException;

import java.time.Duration;
import java.time.Instant;

/**
 * Tiene vivo — e verifica — il possesso di una giornata per tutta la sua lavorazione.
 *
 * <h2>Due metodi, perché sono due cose diverse</h2>
 *
 * <ul>
 *   <li>{@link #requireOwnership()} è la <strong>chiusura</strong>: obbligatoria, non rimandabile e
 *       sempre eseguita, dentro la transazione che scrive dato business o che svuota una
 *       partizione. Se la giornata non è più nostra, solleva e la scrittura non avviene;</li>
 *   <li>{@link #renewIfDue(Instant)} è il <strong>battito</strong>: opportunistico, per le fasi che
 *       non hanno una transazione business — discovery, preflight, il passaggio da un file al
 *       successivo. Serve a non far scadere il lease mentre si lavora davvero.</li>
 * </ul>
 *
 * <p>Il battito è limitato da {@code fdr.recovery.heartbeat-interval} e la chiusura no, ed è
 * voluto: la chiusura è una garanzia di correttezza e non può essere saltata per risparmiare una
 * scrittura, mentre il battito è manutenzione e ripeterlo ogni pochi millisecondi non aggiunge
 * nulla. Il costo della chiusura resta per batch, mai per riga: su una giornata reale sono circa
 * 816 batch, cioè 1.632 aggiornamenti di riga per chiave primaria contro 810.492 INSERT di dato
 * business.
 *
 * <h2>Non è thread-safe, e non deve esserlo</h2>
 *
 * <p>Una giornata è lavorata da un thread solo. Rendere sincronizzato l'istante dell'ultimo battito
 * suggerirebbe il contrario, e nasconderebbe un uso che sarebbe comunque sbagliato per altri motivi
 * — due thread sulla stessa giornata si sovrascriverebbero i contatori molto prima di avere un
 * problema con questo campo.
 */
public final class FdrLeaseGuard implements FdrDayFence {

    private final FdrDayOwnership ownership;
    private final FdrDayLease lease;
    private final Duration heartbeatInterval;

    /** Quando il lease è stato rinnovato l'ultima volta; all'inizio è l'istante del claim. */
    private Instant lastRenewedAt;

    FdrLeaseGuard(FdrDayOwnership ownership, FdrDayLease lease, Duration heartbeatInterval,
                  Instant claimedAt) {
        this.ownership = ownership;
        this.lease = lease;
        this.heartbeatInterval = heartbeatInterval;
        this.lastRenewedAt = claimedAt;
    }

    /**
     * Conferma il possesso e rinnova il lease, o solleva.
     *
     * <p>L'UPDATE che esegue tocca entrambe le righe della giornata e <strong>ne prende i
     * lock</strong>: finché la transazione chiamante non committa, un subentro concorrente resta in
     * attesa. È questo, e non la frequenza del battito, a rendere impossibile che due worker
     * scrivano la stessa giornata.
     */
    @Override
    public void requireOwnership() {
        Instant now = ownership.now();
        if (!ownership.renew(lease, now)) {
            throw new FdrLeaseLostException(lease.batchId(), lease.businessDay(), lease.token());
        }
        lastRenewedAt = now;
    }

    /**
     * Rinnova il lease se è passato abbastanza tempo, senza sollevare.
     *
     * <p>Non solleva di proposito: è chiamato in punti in cui non si sta scrivendo nulla, e lì la
     * perdita del lease non è ancora un problema — lo diventa al primo tentativo di scrittura, dove
     * c'è {@link #requireOwnership()}. Trasformare un battito mancato in un'eccezione
     * interromperebbe la lavorazione in un punto arbitrario, invece che nell'unico punto in cui
     * l'interruzione è sicura.
     *
     * @return {@code true} se il possesso risulta ancora valido, {@code false} se risulta perso.
     *         Chi vuole fermarsi subito può guardarlo; chi non lo guarda si ferma comunque alla
     *         scrittura successiva
     */
    public boolean renewIfDue(Instant now) {
        if (now.isBefore(lastRenewedAt.plus(heartbeatInterval))) {
            return true;
        }
        boolean stillOwned = ownership.renew(lease, now);
        if (stillOwned) {
            lastRenewedAt = now;
        }
        return stillOwned;
    }
}
