package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrLeaseLostException;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.Set;
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
    private final FdrDayOwnership ownership;
    private final FdrIngestionStatusRepository statusRepository;
    private final FdrIngestionEventRepository eventRepository;

    /**
     * Da quali stati il DAILY puo' prendere in carico una giornata.
     *
     * <p>{@code PENDING} e' la giornata mai vista, che e' il caso normale. {@code PROCESSING} c'e'
     * perche' una giornata abbandonata da un pod morto deve poter essere ripresa — e non e' un
     * permesso pericoloso: il claim concede {@code PROCESSING} solo quando il lease e' scaduto,
     * quindi una giornata ancora viva non viene toccata.
     *
     * <p>Non ci sono {@code WAITING_RETRY} ne' {@code FAILED}, ed e' deliberato: il primo ha gia'
     * un orario per il tentativo successivo e ripartire adesso lo anticiperebbe ignorando
     * l'intervallo; il secondo e' un fallimento terminale, che vuole una decisione umana, cioe' una
     * REINGESTION. {@code MISSING} nemmeno: i file di quella giornata non sono mai stati
     * pubblicati, e riprovare ogni notte non li farebbe comparire.
     */
    private static final Set<FdrProcessingStatus> DAILY_CLAIMABLE =
            EnumSet.of(FdrProcessingStatus.PENDING, FdrProcessingStatus.PROCESSING);
    private final FdrProperties.Daily daily;
    private final Clock clock;

    /**
     * Il costruttore di produzione.
     *
     * <p><strong>{@code @Autowired} qui non e' decorativo e non va tolto.</strong> La classe ha due
     * costruttori — questo e quello con il {@link Clock} usato dai test — e quando i costruttori sono
     * piu' di uno Spring non ne sceglie nessuno: ricade sul costruttore senza argomenti, che non
     * esiste, e il contesto non parte con
     * {@code BeanInstantiationException: No default constructor found}. Con un solo costruttore
     * l'annotazione sarebbe superflua; con due e' cio' che tiene in piedi l'applicazione.
     */
    @Autowired
    public FdrDailyService(FdrDayProcessor dayProcessor, FdrBatchRepository batchRepository,
                           FdrDayOwnership ownership,
                           FdrIngestionStatusRepository statusRepository,
                           FdrIngestionEventRepository eventRepository,
                           FdrProperties fdrProperties) {
        this(dayProcessor, batchRepository, ownership, statusRepository, eventRepository,
                fdrProperties,
                Clock.system(ZoneId.of(fdrProperties.getDaily().getTimezone())));
    }

    /** Seam per i test: l'orologio e' l'unica dipendenza non deterministica di questa classe. */
    FdrDailyService(FdrDayProcessor dayProcessor, FdrBatchRepository batchRepository,
                    FdrDayOwnership ownership, FdrIngestionStatusRepository statusRepository,
                    FdrIngestionEventRepository eventRepository, FdrProperties fdrProperties,
                    Clock clock) {
        this.dayProcessor = dayProcessor;
        this.batchRepository = batchRepository;
        this.ownership = ownership;
        this.statusRepository = statusRepository;
        this.eventRepository = eventRepository;
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
        if (existing != null && !isResumable(existing)) {
            logSkip(businessDay, existing);
            return Optional.empty();
        }

        long batchId = batchRepository.createProcessing(FdrWorkflow.DAILY, businessDay, businessDay,
                "scheduler", now);
        // Il claim e' l'ultima parola, non il controllo qui sopra: quello guarda l'esito aggregato
        // delle esecuzioni precedenti, questo prende davvero il possesso delle due righe della
        // giornata e fallisce se un altro pod e' arrivato prima o la sta ancora lavorando.
        FdrDayLease lease;
        try {
            lease = ownership.claim(batchId, businessDay, DAILY_CLAIMABLE, now).orElse(null);
        } catch (FdrInconsistentDayException e) {
            // Non e' un guasto del DAILY: e' una giornata che qualcuno ha lasciato incoerente. Si
            // chiude e si segnala, e il batch appena creato si chiude come fallito perche' non ha
            // prodotto nulla.
            FdrDayInconsistency.settle(eventRepository, statusRepository, batchId, businessDay, e,
                    now);
            batchRepository.complete(batchId, FdrProcessingStatus.FAILED, now);
            return Optional.empty();
        }
        if (lease == null) {
            batchRepository.complete(batchId, FdrProcessingStatus.COMPLETED, now);
            logSkip(businessDay, existing == null ? FdrProcessingStatus.PROCESSING : existing);
            return Optional.empty();
        }

        try {
            FdrDayResult result = dayProcessor.process(lease, FdrDayScope.RESUME);
            batchRepository.complete(batchId, result.status(), now);
            return Optional.of(result);
        } catch (FdrLeaseLostException e) {
            // Un altro worker ha ripreso la giornata mentre la lavoravamo. Non e' un guasto e non
            // va scritto come tale: lo stato della giornata ora descrive il lavoro di quell'altro,
            // e toccarlo lo cancellerebbe. Qui resta una riga di log e nient'altro.
            logLeaseLost(businessDay);
            batchRepository.complete(batchId, FdrProcessingStatus.COMPLETED, clock.instant());
            return Optional.empty();
        }
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
     *   <li>{@code PROCESSING}: dipende, e a deciderlo non e' questo metodo ma il claim. Qui si
     *       dice soltanto "vale la pena provarci"; se la giornata e' davvero posseduta da un pod
     *       vivo, il lease non e' scaduto e il claim fallisce. E' una divisione voluta: la liveness
     *       e' un fatto registrato sulla riga, non una stima fatta prima di guardarla.</li>
     * </ul>
     */
    private static boolean isResumable(FdrProcessingStatus status) {
        return status == FdrProcessingStatus.PROCESSING;
    }

    private static void logLeaseLost(LocalDate businessDay) {
        try (MdcScope ignored = MdcScope.phase("FDR_DAILY");
             LogDetails details = LogDetails.open()
                     .with("business_day", businessDay.toString())) {
            log.warn("Stopped the FDR daily run: another worker took over the day");
        }
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
