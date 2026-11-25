package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Esegue le richieste amministrative che il backend lascia a database.
 *
 * <p>Il contratto e' volutamente asincrono: il backend <strong>scrive una riga</strong> e non
 * chiama nessuno; l'Ingestor la trova, se la prende e la esegue. Nessuno dei due deve essere su
 * perche' l'altro funzioni, e una richiesta fatta mentre l'Ingestor e' fermo viene semplicemente
 * raccolta al riavvio.
 *
 * <p>Due workflow, con una differenza che vale la pena tenere a mente:
 * <ul>
 *   <li><strong>BOOTSTRAP</strong> arriva come intervallo e viene espanso qui. La granularita' per
 *       dataset resta un dettaglio interno nostro;</li>
 *   <li><strong>REINGESTION</strong> arriva con le giornate gia' esplicite, perche' possono essere
 *       <em>sparse</em> — 10/10, 15/11, 10/12 — e un intervallo direbbe un'altra cosa.</li>
 * </ul>
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrCommandService {

    private final FdrBatchRepository batchRepository;
    private final FdrDayProcessor dayProcessor;
    private final FdrReingestionPreparer reingestionPreparer;
    private final FdrIngestionEventRepository eventRepository;
    private final FdrStaleProcessingDetector staleDetector;
    private final FdrIngestionStatusRepository statusRepository;
    private final FdrRetryPolicy retryPolicy;
    private final Duration staleAfter;
    private final Clock clock;

    public FdrCommandService(FdrBatchRepository batchRepository, FdrDayProcessor dayProcessor,
                             FdrReingestionPreparer reingestionPreparer,
                             FdrIngestionEventRepository eventRepository,
                             FdrStaleProcessingDetector staleDetector,
                             FdrIngestionStatusRepository statusRepository,
                             FdrRetryPolicy retryPolicy, FdrProperties fdrProperties) {
        this(batchRepository, dayProcessor, reingestionPreparer, eventRepository, staleDetector,
                statusRepository, retryPolicy, fdrProperties, Clock.systemUTC());
    }

    FdrCommandService(FdrBatchRepository batchRepository, FdrDayProcessor dayProcessor,
                      FdrReingestionPreparer reingestionPreparer,
                      FdrIngestionEventRepository eventRepository,
                      FdrStaleProcessingDetector staleDetector,
                      FdrIngestionStatusRepository statusRepository, FdrRetryPolicy retryPolicy,
                      FdrProperties fdrProperties, Clock clock) {
        this.batchRepository = batchRepository;
        this.dayProcessor = dayProcessor;
        this.reingestionPreparer = reingestionPreparer;
        this.eventRepository = eventRepository;
        this.staleDetector = staleDetector;
        this.statusRepository = statusRepository;
        this.retryPolicy = retryPolicy;
        this.staleAfter = fdrProperties.getRecovery().getStaleAfter();
        this.clock = clock;
    }

    /**
     * Un giro di polling: prende e porta a termine le richieste in attesa.
     *
     * @return quante richieste sono state eseguite da questo worker
     */
    public int pollAndRun() {
        recoverAbandoned();
        List<FdrBatchRepository.FdrBatchRow> pending = batchRepository.findPendingCommands();
        int executed = 0;
        for (FdrBatchRepository.FdrBatchRow batch : pending) {
            // Il claim e' la riga che rende sicuro il multi-pod: chi perde non aspetta, passa
            // semplicemente alla richiesta successiva.
            if (!batchRepository.claim(batch.id(), clock.instant())) {
                continue;
            }
            run(batch);
            executed++;
        }
        return executed;
    }

    /**
     * Rimette in coda le richieste di un pod che non c'e' piu'.
     *
     * <p>Una richiesta ferma su {@code PROCESSING} non si chiude da sola e nessun altro la prende,
     * perche' il polling guarda solo {@code PENDING}: senza questo passaggio resterebbe li' per
     * sempre, apparentemente in lavorazione.
     *
     * <p>Il tempo trascorso non basta a dichiararla abbandonata — una giornata grande e' lenta per
     * conto suo — quindi la prova la da' l'heartbeat delle esecuzioni FDR in
     * {@code INGEST_EXECUTION_LOG}, che e' lo stesso segnale su cui il reaper dell'AS-IS marca
     * {@code ABORTED}.
     *
     * <p>Una REINGESTION cosi' recuperata <strong>rifa' la giornata da capo</strong>: preflight,
     * truncate delle due partizioni, elaborazione. E' l'unica forma corretta, perche' il tentativo
     * interrotto potrebbe aver gia' svuotato la giornata e riscritto solo un pezzo, e nessuno sa
     * quale.
     */
    private void recoverAbandoned() {
        Instant now = clock.instant();
        List<FdrBatchRepository.FdrBatchRow> abandoned =
                batchRepository.findStaleProcessing(now.minus(staleAfter));
        if (abandoned.isEmpty() || staleDetector.isAnyFdrExecutionAlive(now)) {
            return;
        }
        for (FdrBatchRepository.FdrBatchRow batch : abandoned) {
            if (!batchRepository.releaseToPending(batch.id(), now)) {
                continue;
            }
            eventRepository.record(batch.id(), null, null, null, null,
                    FdrEventType.TECHNICAL_ERROR, FdrEventType.WARNING, "PROCESSING_ABANDONED",
                    "Request requeued after the worker that owned it stopped reporting", null, now);
            try (MdcScope ignored = MdcScope.phase("FDR_COMMAND");
                 LogDetails details = LogDetails.open()
                         .with("batch_id", batch.id())
                         .with("workflow", batch.workflow().name())) {
                log.warn("Requeued an abandoned FDR request");
            }
        }
    }

    private void run(FdrBatchRepository.FdrBatchRow batch) {
        Instant now = clock.instant();
        List<LocalDate> days = batch.workflow() == FdrWorkflow.BOOTSTRAP
                ? expandBootstrap(batch, now)
                : batchRepository.findBusinessDays(batch.id());

        if (days.isEmpty()) {
            reject(batch, "EMPTY_REQUEST", "the request carries no business day", now);
            return;
        }
        if (batch.workflow() == FdrWorkflow.REINGESTION && !isValid(batch, days, now)) {
            return;
        }

        for (LocalDate day : days) {
            runDay(batch, day);
        }
        batchRepository.complete(batch.id(),
                FdrBatchRepository.aggregate(batchRepository.findStatuses(batch.id())), now);
    }

    /**
     * Il BOOTSTRAP arriva come intervallo e viene espanso in giornate, in ordine cronologico.
     *
     * <p>L'espansione avviene <strong>dopo</strong> il claim: se la facesse il backend dovrebbe
     * conoscere la granularita' per dataset, che e' una nostra scelta interna e che un giorno
     * potrebbe cambiare senza che lui ne sappia nulla.
     */
    private List<LocalDate> expandBootstrap(FdrBatchRepository.FdrBatchRow batch, Instant now) {
        if (batch.requestedFrom() == null || batch.requestedTo() == null
                || batch.requestedTo().isBefore(batch.requestedFrom())) {
            return List.of();
        }
        List<LocalDate> days = new ArrayList<>();
        for (LocalDate day = batch.requestedFrom(); !day.isAfter(batch.requestedTo());
                day = day.plusDays(1)) {
            for (FdrDataset dataset : FdrDataset.values()) {
                batchRepository.createStatusIfAbsent(batch.id(), day, dataset, now);
            }
            days.add(day);
        }
        return days;
    }

    /**
     * Una REINGESTION vale solo se ogni giornata porta entrambi i dataset.
     *
     * <p>La giornata e' l'unita' atomica: non esiste reingerire i soli IUV. Una richiesta che ne
     * elenca uno solo e' malformata, e viene respinta <strong>prima</strong> di qualunque
     * operazione distruttiva — non a meta', quando una partizione e' gia' stata svuotata.
     */
    private boolean isValid(FdrBatchRepository.FdrBatchRow batch, List<LocalDate> days, Instant now) {
        for (LocalDate day : days) {
            List<String> datasets = batchRepository.findDatasets(batch.id(), day);
            boolean complete = datasets.size() == FdrDataset.values().length
                    && datasets.containsAll(List.of(FdrDataset.FLUSSI_RENDICONTAZIONE.name(),
                            FdrDataset.IUV_RENDICONTATI.name()));
            if (!complete) {
                reject(batch, "INVALID_REINGESTION_REQUEST",
                        "day " + day + " does not carry exactly one status per dataset: " + datasets,
                        now);
                return false;
            }
        }
        return true;
    }

    /**
     * Una giornata di una richiesta.
     *
     * <p>Per la REINGESTION si passa prima dal preflight e dal truncate; se il preflight dice che
     * la giornata non e' ricostruibile, <strong>non si tocca nulla</strong> e si prosegue con le
     * altre: le giornate sparse sono indipendenti, e il fatto che il 15/11 non sia ricostruibile
     * non e' una ragione per lasciare il 10/12 com'era.
     */
    private void runDay(FdrBatchRepository.FdrBatchRow batch, LocalDate day) {
        Instant now = clock.instant();
        if (batch.workflow() == FdrWorkflow.REINGESTION) {
            FdrReingestionPreparer.FdrPreflightResult preflight;
            try {
                preflight = reingestionPreparer.preflight(day);
            } catch (RuntimeException e) {
                // Un guasto tecnico non e' un "non ricostruibile": non si sa ancora nulla della
                // sorgente, quindi la giornata resta intatta e va ritentata piu' tardi.
                eventRepository.record(batch.id(), null, day, null, null,
                        FdrEventType.TECHNICAL_ERROR, FdrEventType.ERROR,
                        e.getClass().getSimpleName(), "Reingestion preflight failed",
                        e.getMessage(), now);
                settleRefusedDay(batch, day, FdrProcessingStatus.WAITING_RETRY,
                        e.getClass().getSimpleName(), e.getMessage(),
                        retryPolicy.nextAttemptAt(now), now);
                return;
            }
            if (!preflight.rebuildable()) {
                eventRepository.record(batch.id(), null, day, preflight.dataset(), null,
                        FdrEventType.FILE_STRUCTURAL_ERROR, FdrEventType.ERROR,
                        preflight.reasonCode(),
                        "Reingestion refused: the day is not rebuildable from the source",
                        preflight.reason(), now);
                settleRefusedDay(batch, day, FdrProcessingStatus.FAILED, preflight.reasonCode(),
                        preflight.reason(), null, now);
                return;
            }
            reingestionPreparer.guardAndTruncate(day);
        }
        dayProcessor.process(batch.id(), day);
    }

    /**
     * Scrive l'esito sulla giornata che non e' stata elaborata.
     *
     * <p>Senza questo passaggio le sue righe resterebbero {@code PENDING}: la giornata sembrerebbe
     * ancora da fare per sempre, e l'esito complessivo del batch — che si compone dagli status —
     * non terrebbe conto del rifiuto. Il dato business non viene toccato: la giornata non e' mai
     * stata svuotata, perche' il preflight sta prima del truncate.
     */
    private void settleRefusedDay(FdrBatchRepository.FdrBatchRow batch, LocalDate day,
                                  FdrProcessingStatus status, String errorCode, String errorMessage,
                                  Instant nextAttemptAt, Instant now) {
        statusRepository.settleDay(batch.id(), day, FdrProcessingStatus.PENDING, status, errorCode,
                errorMessage, nextAttemptAt, now);
    }

    /**
     * Respinge la richiesta: evento, esito terminale sul batch, una riga di log.
     *
     * <p>La giornata dell'evento resta <strong>nulla</strong>, e non e' una dimenticanza: un rifiuto
     * riguarda la richiesta nel suo insieme, non una giornata. Scriverci la data odierna - come
     * faceva la prima stesura - significherebbe affermare che e' successo qualcosa a <em>quella</em>
     * giornata, e chi poi filtrasse gli eventi per giorno se la ritroverebbe senza capire perche'.
     * La colonna e' nullable proprio per questo.
     */
    private void reject(FdrBatchRepository.FdrBatchRow batch, String code, String detail,
                        Instant now) {
        eventRepository.record(batch.id(), null, null, null, null,
                FdrEventType.TECHNICAL_ERROR, FdrEventType.ERROR, code,
                "Administrative request refused", detail, now);
        batchRepository.complete(batch.id(), FdrProcessingStatus.FAILED, now);
        try (MdcScope ignored = MdcScope.phase("FDR_COMMAND");
             LogDetails details = LogDetails.open()
                     .with(LogFields.EVENT_OUTCOME, "failure")
                     .with("batch_id", batch.id())
                     .with("workflow", batch.workflow().name())
                     .with(LogFields.REASON, code)) {
            log.warn("Refused an FDR administrative request");
        }
    }
}
