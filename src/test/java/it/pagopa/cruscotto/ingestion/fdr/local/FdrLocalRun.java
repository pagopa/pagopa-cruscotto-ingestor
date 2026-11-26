package it.pagopa.cruscotto.ingestion.fdr.local;

import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrCommandService;
import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrDailyService;
import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrDayResult;
import it.pagopa.cruscotto.ingestion.fdr.workflow.FdrRetryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;


/**
 * Punto d'ingresso manuale per provare FDR in locale contro il database di DEV.
 *
 * <p><strong>Non è un test.</strong> Non asserisce nulla: avvia il contesto vero e chiama una delle
 * tre porte d'ingresso del modulo, esattamente come farebbe il job Quartz. Sta fra i test perché è
 * il modo più semplice per avere un contesto Spring completo senza aggiungere codice di servizio
 * all'applicazione — una classe di avvio in {@code src/main} esisterebbe anche in produzione, e
 * basterebbe una property sbagliata per farla partire là.
 *
 * <p><strong>È escluso dalla suite per costruzione</strong>, non per convenzione: senza
 * {@code -Dfdr.local=true} JUnit lo salta. In CI quella property non c'è, quindi non può accendersi
 * per distrazione — e se si accendesse non troverebbe il profilo né le credenziali.
 *
 * <p><strong>Scrive sul database di dev.</strong> Una giornata completa sono circa 810.000 righe
 * nelle due tabelle business. La scrittura è un INSERT fedele, senza deduplica: rilanciare la
 * stessa giornata la <em>raddoppia</em>. Per ripetere una prova serve una REINGESTION, che svuota
 * prima di riscrivere, oppure la pulizia manuale in {@code .handover/fdr-prova-locale.sql}.
 *
 * <h2>Come si lancia</h2>
 *
 * <pre>
 * mvn -o test -Dtest=FdrLocalRun -Dfdr.local=true -Dfdr.local.mode=daily -DfailIfNoTests=false
 * </pre>
 *
 * <p>I profili li dichiara l'annotazione, {@code no-liquibase} compreso: lanciando da qui non si
 * puo' dimenticarlo. Serve perche' il Liquibase dell'applicazione non riconosce i changeSet
 * applicati dal workflow di migration — li registra con un percorso logico diverso — e proverebbe
 * a ricostruire da zero lo schema di dev.
 *
 * I tre valori di {@code fdr.local.mode} corrispondono alle tre porte del modulo:
 * {@code daily}, {@code command}, {@code retry}. Il default è {@code daily}.
 */
@SpringBootTest(classes = it.pagopa.cruscotto.ingestion.IngestorApplicationApp.class)
@ActiveProfiles({"local", "no-liquibase"})
@EnabledIfSystemProperty(named = "fdr.local", matches = "true")
class FdrLocalRun {

    @Autowired
    private FdrDailyService dailyService;

    @Autowired
    private FdrCommandService commandService;

    @Autowired
    private FdrRetryService retryService;

    @Test
    void esegui() {
        String mode = System.getProperty("fdr.local.mode", "daily");
        System.out.printf("%n=== FDR in locale, modalita': %s ===%n", mode);

        switch (mode) {
            case "daily" -> runDaily();
            case "command" -> runCommand();
            case "retry" -> runRetry();
            default -> throw new IllegalArgumentException(
                    "modalita' sconosciuta: " + mode + " (daily | command | retry)");
        }
    }

    /**
     * La giornata configurata in {@code fdr.daily.override-business-day}.
     *
     * <p>Un esito vuoto non è un guasto: è la guardia di idempotenza che ha funzionato, cioè la
     * giornata era già chiusa. È lo stesso comportamento che in DEV si vedrebbe al secondo scatto
     * del cron, e la ragione per cui ripetere una prova richiede una REINGESTION.
     */
    private void runDaily() {
        FdrDayResult day = dailyService.runDaily().orElse(null);
        if (day == null) {
            System.out.println("NOOP: la giornata risulta gia' chiusa. "
                    + "Per rifarla serve una REINGESTION, non un rilancio.");
            return;
        }
        System.out.printf("batch=%d giornata=%s esito=%s X=%d N=%d%n",
                day.batchId(), day.businessDay(), day.status(), day.rowsSource(),
                day.rowsProcessed());
        day.datasets().forEach((dataset, outcome) -> System.out.printf(
                "  %-24s %-24s X=%7d N=%7d scartate=%d%n", dataset, outcome.status(),
                outcome.rowsSource(), outcome.rowsProcessed(), outcome.rowsFailed()));
    }

    /**
     * Un giro del poller delle richieste amministrative: BOOTSTRAP e REINGESTION.
     *
     * <p>Prima di lanciarlo va scritta la richiesta a database — è il contratto vero, quello che
     * userà il backend. Le INSERT pronte stanno in {@code .handover/fdr-prova-locale.sql}.
     */
    private void runCommand() {
        int eseguite = commandService.pollAndRun();
        System.out.printf("richieste prese in carico ed eseguite: %d%n", eseguite);
        if (eseguite == 0) {
            System.out.println("Nessuna richiesta PENDING: controlla di aver inserito il batch "
                    + "e i due status per ogni giornata.");
        }
    }

    /** Un giro del poller dei retry: riprende le giornate il cui tentativo e' scaduto. */
    private void runRetry() {
        int riprese = retryService.pollAndRetry();
        System.out.printf("giornate riprese: %d%n", riprese);
        if (riprese == 0) {
            System.out.println("Nessuna giornata scaduta: serve uno status WAITING_RETRY con "
                    + "NEXT_ATTEMPT_AT nel passato e ATTEMPT_COUNT sotto il massimo.");
        }
    }
}
