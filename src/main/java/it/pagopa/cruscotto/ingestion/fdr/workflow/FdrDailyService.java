package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;

/**
 * L'elaborazione giornaliera: la sola che l'Ingestor avvia da solo.
 *
 * <p>Alle 05:00 elabora <strong>la giornata precedente</strong>, perche' i file di una giornata
 * finiscono di essere pubblicati dopo la mezzanotte e non prima.
 *
 * <p>Qui, e solo qui, si guarda l'orologio. Il core riceve sempre una data esplicita: e' cio' che
 * rende possibili BOOTSTRAP e REINGESTION su giornate storiche, e che rende i test del core
 * indipendenti dal calendario.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrDailyService {

    private final FdrDayProcessor dayProcessor;
    private final FdrBatchRepository batchRepository;
    private final FdrStaleProcessingDetector staleDetector;
    private final FdrProperties.Daily daily;
    private final Clock clock;

    public FdrDailyService(FdrDayProcessor dayProcessor, FdrBatchRepository batchRepository,
                           FdrStaleProcessingDetector staleDetector, FdrProperties fdrProperties) {
        this(dayProcessor, batchRepository, staleDetector, fdrProperties,
                Clock.system(ZoneId.of(fdrProperties.getDaily().getTimezone())));
    }

    /** Seam per i test: l'orologio e' l'unica dipendenza non deterministica di questa classe. */
    FdrDailyService(FdrDayProcessor dayProcessor, FdrBatchRepository batchRepository,
                    FdrStaleProcessingDetector staleDetector, FdrProperties fdrProperties,
                    Clock clock) {
        this.dayProcessor = dayProcessor;
        this.batchRepository = batchRepository;
        this.staleDetector = staleDetector;
        this.daily = fdrProperties.getDaily();
        this.clock = clock;
    }

    /**
     * Elabora la giornata di competenza, se non e' gia' stata chiusa.
     *
     * @return l'esito, oppure vuoto se non c'era nulla da fare
     */
    public Optional<FdrDayResult> runDaily() {
        LocalDate businessDay = targetBusinessDay();
        Instant now = clock.instant();

        FdrProcessingStatus existing = batchRepository.findDailyOutcome(businessDay).orElse(null);
        if (existing != null && !shouldRun(existing, now)) {
            logSkip(businessDay, existing);
            return Optional.empty();
        }

        long batchId = batchRepository.createProcessing(FdrWorkflow.DAILY, businessDay, businessDay,
                "scheduler", now);
        FdrDayResult result = dayProcessor.process(batchId, businessDay);
        batchRepository.complete(batchId, result.status(), now);
        return Optional.of(result);
    }

    /**
     * La giornata da elaborare: ieri, salvo override.
     *
     * <p>L'override serve a DEV, dove aspettare le 05:00 e il cambio di giorno per provare una
     * modifica non e' praticabile. E' una property e non un ramo {@code if (dev)} nel codice:
     * cosi' DEV e produzione percorrono le stesse righe, ed e' la configurazione a cambiare, non
     * il comportamento.
     */
    LocalDate targetBusinessDay() {
        String override = daily.getOverrideBusinessDay();
        if (override != null && !override.isBlank()) {
            return LocalDate.parse(override.trim());
        }
        return LocalDate.now(clock).minusDays(1);
    }

    /**
     * Se vale la pena avviare una nuova elaborazione per una giornata gia' vista.
     *
     * <ul>
     *   <li>chiusa con successo: no. Rielaborarla raddoppierebbe le righe, perche' la scrittura e'
     *       un INSERT fedele senza deduplica. Si sovrascrive solo con una REINGESTION;</li>
     *   <li>{@code WAITING_RETRY}: no. Il tentativo successivo ha gia' un orario, e il poller dei
     *       retry lo rispettera'; il DAILY che ripartisse adesso lo anticiperebbe ignorando
     *       l'intervallo;</li>
     *   <li>{@code FAILED}: no. Un fallimento terminale non si ritenta da solo, serve una decisione
     *       umana — e quella decisione e' una REINGESTION;</li>
     *   <li>{@code PROCESSING}: dipende. Se c'e' un'esecuzione viva si lascia stare; se non c'e'
     *       piu' nessuno, la giornata e' stata abbandonata da un pod morto e va ripresa.</li>
     * </ul>
     */
    private boolean shouldRun(FdrProcessingStatus status, Instant now) {
        if (status == FdrProcessingStatus.PROCESSING) {
            return !staleDetector.isAnyFdrExecutionAlive(now);
        }
        return false;
    }

    private static void logSkip(LocalDate businessDay, FdrProcessingStatus status) {
        try (MdcScope ignored = MdcScope.phase("FDR_DAILY");
             LogDetails details = LogDetails.open()
                     .with("business_day", businessDay.toString())
                     .with("existing_status", status.name())) {
            log.info("Skipped the FDR daily run, the day is already settled");
        }
    }
}
