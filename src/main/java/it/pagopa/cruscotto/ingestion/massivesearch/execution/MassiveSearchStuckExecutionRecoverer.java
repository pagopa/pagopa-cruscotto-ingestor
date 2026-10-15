package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import lombok.extern.slf4j.Slf4j;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Recovers Massive Search executions stuck in {@code RUNNING} (e.g. because the JVM/pod died during
 * processing). Without this, with per-instance concurrency disabled, the instance would stay
 * {@code RUNNING} forever and never be executed again.
 *
 * <p>Invoked by the scanner before it looks for new READY instances. Every transition is a
 * conditional DB update, so a genuinely completing execution is never clobbered.</p>
 *
 * <p><strong>Una transazione per esecuzione.</strong> Il recupero sposta <em>due</em> righe —
 * l'esecuzione a FAILED e la sua istanza da RUNNING a FAILED — e le due devono cadere insieme.
 * Senza transazione, morire fra i due UPDATE (il pod viene terminato esattamente mentre si
 * recupera da un pod terminato: non e' un caso remoto) lasciava l'esecuzione in uno stato
 * terminale e l'istanza bloccata in {@code RUNNING}. Quella combinazione e' <strong>irrecuperabile
 * da sola</strong>: alla scansione successiva l'esecuzione non e' piu' RUNNING, quindi
 * {@code findStuckRunningExecutions} non la restituisce e nessuno torna a guardare l'istanza, che
 * resta ferma finche' qualcuno non la sblocca a mano sul database.</p>
 *
 * <p>La transazione e' per <em>singola</em> esecuzione e non per l'intero ciclo: una riga che
 * fallisce non deve far rinunciare al recupero delle altre.</p>
 */
@Slf4j
@Service
public class MassiveSearchStuckExecutionRecoverer {

    private static final String STUCK_ERROR_CODE = "STUCK_EXECUTION";

    private final MassiveSearchProperties properties;
    private final SearchExecutionRepository executionRepository;
    private final SearchInstanceRepository instanceRepository;
    private final TransactionTemplate transactionTemplate;

    public MassiveSearchStuckExecutionRecoverer(
        MassiveSearchProperties properties,
        SearchExecutionRepository executionRepository,
        SearchInstanceRepository instanceRepository,
        PlatformTransactionManager transactionManager
    ) {
        this.properties = properties;
        this.executionRepository = executionRepository;
        this.instanceRepository = instanceRepository;
        // TransactionTemplate e non @Transactional: il metodo transazionale sarebbe invocato dal
        // ciclo della stessa classe, e un'auto-invocazione non passa dal proxy Spring — la
        // transazione non verrebbe aperta affatto, in silenzio.
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /**
     * Fails every stuck execution and its owning instance.
     *
     * @return the number of executions actually recovered
     */
    public int recoverStuckExecutions() {
        int timeoutMinutes = Math.max(1, properties.getExecution().getRunningTimeoutMinutes());
        OffsetDateTime threshold = OffsetDateTime.now().minusMinutes(timeoutMinutes);

        try (MdcScope ignored = MdcScope.phase("STUCK_EXECUTION_RECOVERY_START")
                     .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH");
             LogDetails details = LogDetails.open()
                     .with("timeout_minutes", timeoutMinutes)
                     .with("threshold", threshold)) {
            log.debug("Started stuck-execution recovery");
        }

        List<StuckExecutionRef> stuck = executionRepository.findStuckRunningExecutions(threshold);
        int recovered = 0;
        for (StuckExecutionRef ref : stuck) {
            try (MdcScope ignored = MdcScope.open().with(LogFields.CTX_INSTANCE_ID, String.valueOf(ref.instanceId())).with(LogFields.CTX_EXECUTION_ID, String.valueOf(ref.executionId()))) {
                if (recoverOne(ref)) {
                    recovered++;
                }
            } catch (RuntimeException e) {
                try (MdcScope phase = MdcScope.phase("STUCK_EXECUTION_RECOVERY_ERROR")
                             .with(LogFields.EVENT_OUTCOME, "failure");
                     LogDetails details = LogDetails.open().with(LogFields.ERROR_DETAIL, e.getMessage())) {
                    log.error("Failed to recover a stuck execution", e);
                }
            }
        }

        try (MdcScope ignored = MdcScope.phase("STUCK_EXECUTION_RECOVERY_COMPLETED")
                     .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH");
             LogDetails details = LogDetails.open()
                     .with("candidates", stuck.size())
                     .with("recovered", recovered)) {
            log.debug("Completed stuck-execution recovery");
        }
        return recovered;
    }

    /**
     * Recupera una singola esecuzione, atomicamente.
     *
     * <p>Il log di esito sta <strong>dentro</strong> la transazione di proposito: se il commit
     * fallisse dopo averlo emesso si avrebbe una riga che dichiara un recupero mai avvenuto. Il
     * contrario — commit riuscito e log perso — e' preferibile, perche' lo stato vero resta quello
     * del database e la scansione successiva lo rilegge coerente.</p>
     *
     * @return {@code true} se l'esecuzione era ancora RUNNING ed e' stata recuperata
     */
    private boolean recoverOne(StuckExecutionRef ref) {
        return Boolean.TRUE.equals(transactionTemplate.execute(status -> {
            executionRepository.applyRecoveryTimeouts();

            boolean executionFailed = executionRepository.recoverStuck(ref.executionId());
            if (!executionFailed) {
                // The execution completed (or was already failed) between the scan and the update.
                try (MdcScope phase = MdcScope.phase("STUCK_EXECUTION_RECOVERY_SKIPPED");
                     LogDetails details = LogDetails.open()
                             .with(LogFields.REASON, "no-longer-running")) {
                    log.info("Skipped recovery, the execution is no longer running");
                }
                return false;
            }

            instanceRepository.markFailedFromRunning(ref.instanceId());

            try (MdcScope phase = MdcScope.phase("STUCK_EXECUTION_RECOVERED")
                         .with(LogFields.EVENT_OUTCOME, "failure");
                 LogDetails details = LogDetails.open().with("error_code", STUCK_ERROR_CODE)) {
                log.warn("Recovered a stuck execution, marked FAILED and instance released");
            }
            return true;
        }));
    }
}
