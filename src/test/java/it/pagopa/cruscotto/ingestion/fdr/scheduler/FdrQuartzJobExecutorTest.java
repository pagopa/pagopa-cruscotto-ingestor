package it.pagopa.cruscotto.ingestion.fdr.scheduler;

import it.pagopa.cruscotto.ingestion.util.LogFields;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.quartz.JobExecutionException;
import org.slf4j.MDC;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Il contorno dei tre job FDR: contesto di log, esito, eccezione preservata.
 *
 * <h2>Cosa c'è da verificare, visto che non scrive da nessuna parte</h2>
 *
 * <p>Proprio questo. Il suo predecessore, {@code TrackedJobExecutor}, apriva e chiudeva una riga in
 * {@code INGEST_EXECUTION_LOG} ed è per quello che i job FDR hanno smesso di usarlo; ciò che resta
 * qui deve fare le tre cose che servivano davvero — correlare i log, dire com'è finita, non perdere
 * la causa — e <strong>niente altro</strong>. Che non scriva su tabelle dell'AS-IS è garantito in
 * modo più forte da {@code FdrOperationalDecouplingTest}, che guarda tutto il modulo; qui si
 * verifica che il poco che fa lo faccia bene.
 */
class FdrQuartzJobExecutorTest {

    private final FdrQuartzJobExecutor executor = new FdrQuartzJobExecutor();

    /**
     * Il contesto c'è <strong>mentre</strong> il corpo gira, non prima o dopo.
     *
     * <p>È la differenza che rende i log correlabili: una riga emessa dal corpo deve portare
     * {@code event_action} e {@code correlation_id} come attributi di primo livello. Il controllo è
     * fatto da dentro il corpo perché guardarlo da fuori direbbe solo che l'MDC è stato ripulito.
     */
    @Test
    @DisplayName("il corpo gira dentro il contesto di log del job")
    void ilCorpoGiraDentroIlContestoDiLog() throws JobExecutionException {
        List<String> seen = new ArrayList<>();

        executor.run("FDR_DAILY", () -> {
            seen.add(MDC.get(LogFields.EVENT_ACTION));
            seen.add(MDC.get(LogFields.CORRELATION_ID));
            seen.add(MDC.get(LogFields.CTX_INSTANCE_ID));
        });

        assertThat(seen.get(0)).isEqualTo("FDR_DAILY");
        assertThat(seen.get(1)).as("il correlation id identifica questa esecuzione").isNotBlank();
        assertThat(seen.get(2)).as("il processo, per il troubleshooting").isNotBlank();
    }

    /**
     * Ogni esecuzione ha il suo correlation id.
     *
     * <p>Riusarne uno renderebbe indistinguibili due giri del poller, che è esattamente ciò che si
     * guarda quando una richiesta sembra non essere stata raccolta. E spiega perché non può fare da
     * token di possesso: due giornate dello stesso giro ne condividerebbero uno.
     */
    @Test
    @DisplayName("ogni esecuzione porta un correlation id diverso")
    void ogniEsecuzioneHaIlSuoCorrelationId() throws JobExecutionException {
        List<String> ids = new ArrayList<>();
        executor.run("FDR_COMMAND", () -> ids.add(MDC.get(LogFields.CORRELATION_ID)));
        executor.run("FDR_COMMAND", () -> ids.add(MDC.get(LogFields.CORRELATION_ID)));

        assertThat(ids).doesNotHaveDuplicates();
    }

    /**
     * Il contesto viene ripulito anche quando il corpo fallisce.
     *
     * <p>I thread di Quartz sono di un pool e vengono riusati: un MDC lasciato sporco farebbe
     * comparire {@code event_action: FDR_DAILY} nei log di un job completamente diverso, e nessuno
     * avrebbe modo di capire da dove arrivi.
     */
    @Test
    @DisplayName("il contesto viene ripulito anche dopo un fallimento")
    void ilContestoVienePulitoAncheDopoUnFallimento() {
        assertThatThrownBy(() -> executor.run("FDR_RETRY", () -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(JobExecutionException.class);

        assertThat(MDC.get(LogFields.EVENT_ACTION)).isNull();
        assertThat(MDC.get(LogFields.CORRELATION_ID)).isNull();
        assertThat(MDC.get(LogFields.CTX_INSTANCE_ID)).isNull();
    }

    /**
     * La causa originale arriva intatta a Quartz.
     *
     * <p>Il rilancio non è una formalità: Quartz distingue un job riuscito da uno fallito solo
     * dall'eccezione, quindi ingoiarla renderebbe invisibile al job store ogni guasto. E la causa
     * va <em>incapsulata</em>, non sostituita, altrimenti chi legge lo stack trace trova questo
     * wrapper al posto del problema.
     */
    @Test
    @DisplayName("il fallimento risale a Quartz con la causa originale")
    void ilFallimentoRisaleConLaCausaOriginale() {
        IllegalStateException cause = new IllegalStateException("azure unreachable");

        assertThatThrownBy(() -> executor.run("FDR_DAILY", () -> {
            throw cause;
        }))
                .isInstanceOf(JobExecutionException.class)
                .hasCause(cause);
    }

    /** Un'esecuzione riuscita non solleva nulla. */
    @Test
    @DisplayName("un'esecuzione riuscita non solleva")
    void unEsecuzioneRiuscitaNonSolleva() throws JobExecutionException {
        executor.run("FDR_COMMAND", () -> {
            // nessun lavoro: interessa solo che non sollevi
        });
    }
}
