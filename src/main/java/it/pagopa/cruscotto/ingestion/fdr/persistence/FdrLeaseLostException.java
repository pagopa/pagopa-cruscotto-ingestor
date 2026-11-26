package it.pagopa.cruscotto.ingestion.fdr.persistence;

import java.time.LocalDate;

/**
 * La giornata non è più di questo worker: un altro ha preso il lease.
 *
 * <h2>Non è un guasto, ed è importante non trattarlo come tale</h2>
 *
 * <p>Quando questa eccezione arriva, da qualche parte le cose stanno andando bene: un altro pod ha
 * rilevato che questo non dava più segni di vita e ha ripreso la giornata. Il comportamento
 * corretto di chi la riceve è <strong>fermarsi e non toccare nulla</strong> — in particolare non
 * scrivere l'esito sulla giornata, che ormai descrive il lavoro di qualcun altro.
 *
 * <p>Per la stessa ragione non va classificata come ritentabile. Un {@code WAITING_RETRY} scritto
 * da un worker che ha perso il lease sovrascriverebbe lo stato di quello che sta lavorando in
 * questo momento, e lo farebbe sembrare fermo in attesa: due worker si ruberebbero la giornata a
 * vicenda invece di sincronizzarsi.
 *
 * <h2>Perché non estende FdrPersistenceException</h2>
 *
 * <p>Quella descrive un problema del dato o del database, e ha una classificazione per riga
 * (isolabile / non isolabile) che qui non avrebbe senso: la perdita del lease non riguarda una
 * riga, riguarda il diritto di scrivere. Tenerle distinte è anche ciò che impedisce al
 * frazionamento del batch di trattarla come un errore di riga e spezzare il batch inutilmente.
 */
public class FdrLeaseLostException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient LocalDate businessDay;

    public FdrLeaseLostException(long batchId, LocalDate businessDay, String token) {
        super("Lease lost on business day " + businessDay + " of batch " + batchId
                + " (token " + token + "): another worker owns it now");
        this.businessDay = businessDay;
    }

    public LocalDate businessDay() {
        return businessDay;
    }
}
