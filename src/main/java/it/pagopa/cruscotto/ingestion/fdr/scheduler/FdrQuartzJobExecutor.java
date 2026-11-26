package it.pagopa.cruscotto.ingestion.fdr.scheduler;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrInstanceId;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.ThrowableDetail;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Il contorno comune dei tre job Quartz FDR: contesto di log, esito, eccezione preservata.
 *
 * <h2>Perché non si usa più {@code TrackedJobExecutor}</h2>
 *
 * <p>Quel wrapper fa una cosa in più che per FDR non è accettabile: {@code runTracked} apre una
 * riga {@code STARTED} in {@code INGEST_EXECUTION_LOG} e la chiude {@code COMPLETED}/{@code FAILED}.
 * È corretto per l'ingestione ADX, dove quella tabella <em>è</em> lo stato dell'esecuzione; per FDR
 * significherebbe tenere lo stato in due posti — là e nelle quattro tabelle operative FDR — con la
 * certezza che prima o poi divergano, e con un ciclo di vita (heartbeat, reaper) che non è quello
 * di FDR. Lo stato di una giornata FDR lo dice {@code FDR_INGESTION_STATUS}, e la liveness la dice
 * il lease: l'execution log non aggiungeva informazione, aggiungeva una seconda verità.
 *
 * <p>{@code TrackedJobExecutor} <strong>non è stato modificato</strong>: continua a servire i dieci
 * job dell'ingestione ADX esattamente come prima.
 *
 * <h2>Cosa fa, e cosa deliberatamente non fa</h2>
 *
 * <p>Fa: genera il correlation id, apre l'MDC, logga l'avvio, esegue, logga l'esito, ripulisce.
 *
 * <p>Non fa: nessuna scrittura su tabelle del vecchio ingestor, nessun heartbeat AS-IS, nessun
 * reaper, e <strong>nessuna ownership</strong>. Il fatto che un job stia girando non dà diritto a
 * scrivere su una giornata: quel diritto è il lease, e si prende sulla giornata, non sul job. È la
 * distinzione che rende innocuo un secondo pod che esegua lo stesso poller nello stesso istante.
 *
 * <h2>Il correlation id non è un token di possesso</h2>
 *
 * <p>Identifica un'esecuzione del job per correlarne i log, e vive solo in memoria e nell'MDC. Non
 * è una FK verso niente, non sopravvive al processo e non dice chi possiede cosa. Usarlo come
 * token di lease sarebbe sbagliato in entrambi i versi: due giornate dello stesso giro ne
 * condividerebbero uno, e un worker riavviato ne userebbe uno nuovo pur avendo ancora il possesso.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrQuartzJobExecutor {

    /** Corpo di un job FDR, che può sollevare qualsiasi cosa. */
    @FunctionalInterface
    public interface FdrJobBody {
        void run() throws Exception;
    }

    /**
     * Esegue il corpo dentro il contesto di log, e <strong>rilancia</strong> ciò che fallisce.
     *
     * <p>Il rilancio non è una formalità: Quartz distingue un job riuscito da uno fallito solo
     * dall'eccezione, e ingoiarla qui renderebbe invisibile al job store ogni guasto. La causa
     * originale viene incapsulata senza essere sostituita, così chi legge lo stack trace trova il
     * problema vero e non questo wrapper.
     *
     * @param jobName il nome del job, che finisce in {@code event_action} ed è ciò su cui si filtra
     * @throws JobExecutionException sempre, quando il corpo fallisce
     */
    public void run(String jobName, FdrJobBody body) throws JobExecutionException {
        String correlationId = UUID.randomUUID().toString();
        try (MdcScope ignored = MdcScope.open()
                .with(LogFields.EVENT_ACTION, jobName)
                .with(LogFields.CORRELATION_ID, correlationId)
                .with(LogFields.CTX_INSTANCE_ID, FdrInstanceId.get())) {
            long startedAt = System.currentTimeMillis();
            try {
                body.run();
            } catch (Exception e) {
                logFailure(jobName, startedAt, e);
                throw new JobExecutionException(e);
            }
            logSuccess(startedAt);
        }
    }

    /**
     * Una riga sola per esecuzione riuscita, con la durata.
     *
     * <p>Niente coppia avvio/fine: su un poller che scatta ogni minuto raddoppierebbe il volume per
     * dire due volte la stessa cosa. Il caso in cui un avvio senza fine sarebbe l'unico segnale —
     * il pod che muore a metà — è già coperto meglio dal lease, che scade e viene visto dal
     * recupero; una riga di log non lo sarebbe stata da nessuno.
     */
    private static void logSuccess(long startedAt) {
        try (MdcScope outcome = MdcScope.open().with(LogFields.EVENT_OUTCOME, "success");
             LogDetails details = LogDetails.open()
                     .with(LogFields.ELAPSED_MS, System.currentTimeMillis() - startedAt)) {
            log.info("Completed FDR job");
        }
    }

    /**
     * L'eccezione viene passata al logger, non concatenata nel messaggio.
     *
     * <p>È ciò che permette all'encoder di produrre {@code error.type}, {@code error.message} e
     * {@code error.stack_trace} strutturati. {@code error_detail} porta in più i guasti collegati
     * che la sola catena delle cause perde — {@code getNextException()} di un batch JDBC, le
     * eccezioni soppresse — che sono spesso l'unica riga utile.
     */
    private static void logFailure(String jobName, long startedAt, Exception e) {
        try (MdcScope outcome = MdcScope.open().with(LogFields.EVENT_OUTCOME, "failure");
             LogDetails details = LogDetails.open()
                     .with("job_name", jobName)
                     .with(LogFields.ELAPSED_MS, System.currentTimeMillis() - startedAt)
                     .with(LogFields.ERROR_DETAIL, ThrowableDetail.format(e))) {
            log.error("FDR job failed", e);
        }
    }
}
