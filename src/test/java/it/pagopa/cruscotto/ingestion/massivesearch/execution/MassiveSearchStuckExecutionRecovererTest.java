package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Il contratto del recupero delle esecuzioni bloccate.
 *
 * <p>I due difetti che questi test esistono per impedire si manifestano entrambi solo in
 * produzione e solo sotto guasto, cioe' nelle condizioni in cui nessuno sta guardando.</p>
 */
class MassiveSearchStuckExecutionRecovererTest {

    private SearchExecutionRepository executionRepository;
    private SearchInstanceRepository instanceRepository;
    private RecordingTransactionManager transactionManager;
    private MassiveSearchStuckExecutionRecoverer recoverer;

    /**
     * Gestore di transazioni che non parla con un database ma registra commit e rollback: basta a
     * verificare che il recupero <em>apra</em> una transazione e che le due scritture ci stiano
     * dentro, che e' il punto in discussione.
     */
    private static final class RecordingTransactionManager implements PlatformTransactionManager {
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger committed = new AtomicInteger();
        private final AtomicInteger rolledBack = new AtomicInteger();

        @Override
        public TransactionStatus getTransaction(org.springframework.transaction.TransactionDefinition definition) {
            started.incrementAndGet();

            return new SimpleTransactionStatus();
        }

        @Override
        public void commit(TransactionStatus status) {
            committed.incrementAndGet();

        }

        @Override
        public void rollback(TransactionStatus status) {
            rolledBack.incrementAndGet();

        }
    }

    @BeforeEach
    void setUp() {
        executionRepository = mock(SearchExecutionRepository.class);
        instanceRepository = mock(SearchInstanceRepository.class);
        transactionManager = new RecordingTransactionManager();

        MassiveSearchProperties properties = new MassiveSearchProperties();
        properties.getExecution().setRunningTimeoutMinutes(60);

        recoverer = new MassiveSearchStuckExecutionRecoverer(
                properties, executionRepository, instanceRepository, transactionManager);
    }

    private StuckExecutionRef stuckOne() {
        StuckExecutionRef ref = new StuckExecutionRef(UUID.randomUUID(), UUID.randomUUID());
        when(executionRepository.findStuckRunningExecutions(any(OffsetDateTime.class)))
                .thenReturn(List.of(ref));
        return ref;
    }

    /**
     * Il difetto numero uno: le due scritture devono stare nella stessa transazione.
     *
     * <p>Senza, morire fra l'{@code UPDATE} dell'esecuzione e quello dell'istanza lasciava
     * l'esecuzione in stato terminale e l'istanza ferma in {@code RUNNING} — combinazione che
     * nessuna scansione successiva puo' sciogliere, perche' il recoverer cerca esecuzioni
     * {@code RUNNING} e quella non lo e' piu'.</p>
     */
    @Test
    void leDueScrittureStannoNellaStessaTransazione() {
        StuckExecutionRef ref = stuckOne();
        when(executionRepository.recoverStuck(ref.executionId())).thenReturn(true);

        assertEquals(1, recoverer.recoverStuckExecutions());

        assertEquals(1, transactionManager.started.get(), "nessuna transazione aperta");
        assertEquals(1, transactionManager.committed.get(), "la transazione non e' stata committata");
        assertEquals(0, transactionManager.rolledBack.get());
        verify(executionRepository).recoverStuck(ref.executionId());
        verify(instanceRepository).markFailedFromRunning(ref.instanceId());
    }

    /**
     * Il difetto numero due: il recupero gira sul thread di scansione di Quartz, quindi deve
     * limitare l'attesa su un lock. Senza, un {@code UPDATE} su una riga bloccata fermerebbe
     * l'intero polling della ricerca massiva a tempo indeterminato.
     *
     * <p>{@code SET LOCAL} ha effetto solo dentro una transazione: va applicato <strong>dopo</strong>
     * che la transazione e' aperta, e prima della prima scrittura.</p>
     */
    @Test
    void ilRecuperoLimitaLAttesaSuiLockDentroLaTransazione() {
        StuckExecutionRef ref = stuckOne();
        when(executionRepository.recoverStuck(ref.executionId())).thenReturn(true);

        recoverer.recoverStuckExecutions();

        InOrder order = inOrder(executionRepository, instanceRepository);
        order.verify(executionRepository).applyRecoveryTimeouts();
        order.verify(executionRepository).recoverStuck(ref.executionId());
        order.verify(instanceRepository).markFailedFromRunning(ref.instanceId());
    }

    /** Un'esecuzione completata fra la scansione e l'update non va toccata, e non si conta. */
    @Test
    void unEsecuzioneNonPiuRunningNonRilasciaLIstanza() {
        StuckExecutionRef ref = stuckOne();
        when(executionRepository.recoverStuck(ref.executionId())).thenReturn(false);

        assertEquals(0, recoverer.recoverStuckExecutions());

        verify(instanceRepository, never()).markFailedFromRunning(any());
    }

    /**
     * Una transazione per esecuzione, non una per ciclo: una riga che fallisce non deve far
     * rinunciare al recupero delle altre.
     */
    @Test
    void unFallimentoNonFermaIlRecuperoDelleAltre() {
        StuckExecutionRef primo = new StuckExecutionRef(UUID.randomUUID(), UUID.randomUUID());
        StuckExecutionRef secondo = new StuckExecutionRef(UUID.randomUUID(), UUID.randomUUID());
        when(executionRepository.findStuckRunningExecutions(any(OffsetDateTime.class)))
                .thenReturn(List.of(primo, secondo));
        when(executionRepository.recoverStuck(primo.executionId()))
                .thenThrow(new IllegalStateException("lock timeout"));
        when(executionRepository.recoverStuck(secondo.executionId())).thenReturn(true);

        assertEquals(1, recoverer.recoverStuckExecutions(), "la seconda doveva essere recuperata");

        assertEquals(2, transactionManager.started.get(), "serve una transazione per esecuzione");
        assertEquals(1, transactionManager.rolledBack.get(), "la prima doveva essere annullata");
        assertEquals(1, transactionManager.committed.get());
        verify(instanceRepository).markFailedFromRunning(secondo.instanceId());
        verify(instanceRepository, never()).markFailedFromRunning(primo.instanceId());
    }

    /** Nessuna esecuzione bloccata: nessuna transazione aperta per niente. */
    @Test
    void senzaCandidatiNonSiApreNessunaTransazione() {
        when(executionRepository.findStuckRunningExecutions(any(OffsetDateTime.class)))
                .thenReturn(List.of());

        assertEquals(0, recoverer.recoverStuckExecutions());

        assertEquals(0, transactionManager.started.get());
    }

    /** La soglia non puo' essere nel futuro: un timeout non positivo verrebbe letto come "tutto bloccato". */
    @Test
    void unTimeoutNonPositivoNonRendeTuttoBloccato() {
        MassiveSearchProperties properties = new MassiveSearchProperties();
        properties.getExecution().setRunningTimeoutMinutes(0);
        MassiveSearchStuckExecutionRecoverer withZero = new MassiveSearchStuckExecutionRecoverer(
                properties, executionRepository, instanceRepository, transactionManager);
        when(executionRepository.findStuckRunningExecutions(any(OffsetDateTime.class)))
                .thenReturn(List.of());

        withZero.recoverStuckExecutions();

        ArgumentCaptor<OffsetDateTime> threshold = ArgumentCaptor.forClass(OffsetDateTime.class);
        verify(executionRepository).findStuckRunningExecutions(threshold.capture());

        assertTrue(threshold.getValue().isBefore(OffsetDateTime.now()),
                "la soglia deve restare nel passato anche con un timeout configurato a zero");
    }
}
