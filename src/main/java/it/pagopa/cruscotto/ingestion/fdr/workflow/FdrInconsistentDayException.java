package it.pagopa.cruscotto.ingestion.fdr.workflow;

import java.time.LocalDate;

/**
 * Le due righe di una giornata non concordano sul numero di tentativo.
 *
 * <h2>Perché è un'eccezione e non un valore da riallineare</h2>
 *
 * <p>L'invariante è {@code FLUSSI.ATTEMPT_COUNT = IUV.ATTEMPT_COUNT}: i due dataset vengono tentati
 * <strong>insieme</strong>, sempre, da una sola {@code UPDATE}. Se divergono non è uno stato
 * normale da cui ripartire — è la prova che qualcosa ha scritto quelle righe fuori dal claim, e
 * quel qualcosa non si conosce.
 *
 * <p>La prima stesura riallineava con {@code max(attempt) + 1}. È sbagliato per una ragione
 * concreta, non estetica: con {@code FLUSSI=1} e {@code IUV=4} il riallineamento porta la giornata
 * a <strong>5</strong>, cioè all'ultimo tentativo disponibile, e il giro successivo la chiude come
 * esaurita. L'anomalia sparirebbe dalla vista proprio mentre consuma i tentativi che servivano a
 * recuperare la giornata.
 *
 * <h2>Non è ritentabile, e non è un WAITING_RETRY</h2>
 *
 * <p>Non è un guasto di Azure né del database: rileggere la stessa sorgente o riprovare fra un'ora
 * ritroverebbe esattamente le stesse due righe discordi. Serve una decisione umana — in pratica una
 * REINGESTION — quindi la giornata viene chiusa come {@code FAILED} con un codice stabile e non
 * rimessa in coda. Lasciarla in attesa produrrebbe un tentativo fallito a ogni giro del poller, per
 * sempre.
 */
public class FdrInconsistentDayException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Il codice che finisce in {@code FDR_INGESTION_EVENT.EVENT_CODE} e in
     * {@code FDR_INGESTION_STATUS.LAST_ERROR_CODE}.
     *
     * <p>Riusa la tassonomia esistente — tipo evento {@code TECHNICAL_ERROR}, severità
     * {@code ERROR} — senza introdurre una famiglia di errori nuova: a distinguerlo è il codice,
     * che è stabile e cercabile.
     */
    public static final String CODE = "INCONSISTENT_DAY_ATTEMPT_COUNT";

    private final transient LocalDate businessDay;

    public FdrInconsistentDayException(long batchId, LocalDate businessDay, String detail) {
        super("Inconsistent attempt count on business day " + businessDay + " of batch " + batchId
                + ": " + detail);
        this.businessDay = businessDay;
    }

    public LocalDate businessDay() {
        return businessDay;
    }
}
