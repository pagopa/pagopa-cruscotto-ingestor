package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrLeaseLostException;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Riprende le giornate che aspettano un tentativo e quelle che un worker ha abbandonato.
 *
 * <p>Due intervalli distinti, e la distinzione conta: si <strong>polla</strong> ogni pochi minuti,
 * si <strong>ritenta</strong> dopo un'ora. Il poller non decide quando ritentare — quello e' scritto
 * in {@code NEXT_ATTEMPT_AT} dal core che ha registrato il guasto — decide solo con quanta
 * puntualita' se ne accorge. Un poll frequente rende il retry puntuale, non frequente.
 *
 * <p>Nessuno {@code sleep}: un thread fermo per un'ora occuperebbe un worker Quartz e non
 * sopravviverebbe comunque a un riavvio del pod. L'attesa sta a database.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrRetryService {

    /**
     * Quale dataset rende <strong>azionabile</strong> una giornata scaduta.
     *
     * <p>Soltanto {@code WAITING_RETRY}, e il motivo e' la simmetria con chi la seleziona: la query
     * cerca le giornate il cui orario e' scaduto, e tutte sono in quello stato. Ammetterne altri
     * qui non allargherebbe il recupero — nessuno gliene passerebbe — ma permetterebbe a una
     * modifica futura della query di far sovrascrivere al retry una giornata in corso senza che
     * nulla lo segnali.
     *
     * <p><strong>Non significa che entrambi i dataset debbano essere in questo stato.</strong> Un
     * guasto a meta' giornata lascia tipicamente {@code FLUSSI = COMPLETED} e
     * {@code IUV = WAITING_RETRY}, e quella giornata deve restare riprendibile: il claim chiede una
     * riga azionabile e le altre concluse, non tutte azionabili. Pretendere l'uniformita' rendeva
     * irrecuperabile proprio il caso piu' frequente — vedi {@code FdrDayOwnership.isDayClaimable}.
     */
    private static final Set<FdrProcessingStatus> RETRY_CLAIMABLE =
            EnumSet.of(FdrProcessingStatus.WAITING_RETRY);

    /**
     * Quale dataset rende azionabile una giornata abbandonata.
     *
     * <p>Solo {@code PROCESSING}, ed e' l'unico punto in cui quello stato e' un punto di partenza
     * legittimo. Non e' un permesso pericoloso: il claim concede {@code PROCESSING} soltanto a
     * lease scaduto, quindi una giornata ancora viva resta intoccata anche se finisse per sbaglio
     * in questa selezione.
     *
     * <p>Vale la stessa precisazione del retry: un pod morto fra un dataset e l'altro lascia la
     * giornata meta' conclusa e meta' in lavorazione, ed e' ancora recuperabile.
     */
    private static final Set<FdrProcessingStatus> ABANDONED_CLAIMABLE =
            EnumSet.of(FdrProcessingStatus.PROCESSING);

    /** Quante giornate abbandonate recuperare per giro: il resto al giro successivo. */
    private static final int ABANDONED_SCAN_LIMIT = 50;

    private final JdbcTemplate jdbcTemplate;
    private final FdrDayProcessor dayProcessor;
    private final FdrReingestionPreparer reingestionPreparer;
    private final FdrRetryPolicy retryPolicy;
    private final FdrIngestionStatusRepository statusRepository;
    private final FdrIngestionEventRepository eventRepository;
    private final FdrDayOwnership ownership;
    private final String schema;
    private final int maxAttempts;
    private final Clock clock;

    /**
     * Il costruttore di produzione.
     *
     * <p><strong>{@code @Autowired} qui non e' decorativo e non va tolto.</strong> La classe ha due
     * costruttori — questo e quello con il {@link Clock} usato dai test — e quando i costruttori
     * sono piu' di uno Spring non ne sceglie nessuno: ricade sul costruttore senza argomenti, che
     * non esiste, e il contesto non parte con
     * {@code BeanInstantiationException: No default constructor found}.
     */
    @Autowired
    public FdrRetryService(JdbcTemplate jdbcTemplate, FdrDayProcessor dayProcessor,
                           FdrReingestionPreparer reingestionPreparer, FdrRetryPolicy retryPolicy,
                           FdrIngestionStatusRepository statusRepository,
                           FdrIngestionEventRepository eventRepository,
                           DbSchemaConfig dbSchemaConfig, FdrProperties fdrProperties,
                           FdrDayOwnership ownership) {
        this(jdbcTemplate, dayProcessor, reingestionPreparer, retryPolicy, statusRepository,
                eventRepository, dbSchemaConfig, fdrProperties, ownership, Clock.systemUTC());
    }

    FdrRetryService(JdbcTemplate jdbcTemplate, FdrDayProcessor dayProcessor,
                    FdrReingestionPreparer reingestionPreparer, FdrRetryPolicy retryPolicy,
                    FdrIngestionStatusRepository statusRepository,
                    FdrIngestionEventRepository eventRepository, DbSchemaConfig dbSchemaConfig,
                    FdrProperties fdrProperties, FdrDayOwnership ownership, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.dayProcessor = dayProcessor;
        this.reingestionPreparer = reingestionPreparer;
        this.retryPolicy = retryPolicy;
        this.statusRepository = statusRepository;
        this.eventRepository = eventRepository;
        this.ownership = ownership;
        this.schema = dbSchemaConfig.getSchemaName();
        this.maxAttempts = fdrProperties.getRetry().getMaxAttempts();
        this.clock = clock;
    }

    /**
     * Un giro del poller: prima le giornate che aspettano un tentativo, poi quelle abbandonate.
     *
     * <p>Le due selezioni sono distinte e tutte e due necessarie. {@code WAITING_RETRY} sono le
     * giornate che hanno fallito e hanno un orario; <strong>abbandonate</strong> sono quelle
     * rimaste in lavorazione con il lease scaduto, cioe' possedute da un worker che non c'e' piu'.
     *
     * <p>Senza la seconda, una giornata DAILY interrotta da un pod morto non verrebbe recuperata da
     * <em>nessuno</em>: il recupero a livello di richiesta guarda solo BOOTSTRAP e REINGESTION — un
     * batch DAILY non vi compare — e il DAILY successivo crea un batch nuovo, lasciando le due
     * righe del precedente ferme su {@code PROCESSING} per sempre.
     *
     * @return quante giornate sono state riprese da questo worker
     */
    public int pollAndRetry() {
        Instant now = clock.instant();
        return resume(findDue(now), RETRY_CLAIMABLE, now)
                + resume(findAbandoned(now), ABANDONED_CLAIMABLE, now);
    }

    /**
     * Riprende un insieme di giornate, qualunque sia la ragione per cui sono state selezionate.
     *
     * <p>Un solo corpo per i due casi: cambia solo lo stato da cui e' ammesso ripartire. Due cicli
     * gemelli sarebbero divergiti alla prima correzione fatta su uno solo dei due.
     */
    private int resume(List<FdrRetryCandidate> candidates, Set<FdrProcessingStatus> eligible,
                       Instant now) {
        int retried = 0;
        for (FdrRetryCandidate candidate : candidates) {
            FdrDayLease lease;
            try {
                lease = ownership
                        .claim(candidate.batchId(), candidate.businessDay(), eligible, now)
                        .orElse(null);
            } catch (FdrInconsistentDayException e) {
                // La chiusura terminale e' cio' che impedisce il giro infinito: senza, il poller
                // ritroverebbe questa giornata ogni cinque minuti e fallirebbe allo stesso modo.
                FdrDayInconsistency.settle(eventRepository, statusRepository,
                        candidate.batchId(), candidate.businessDay(), e, now);
                continue;
            }
            if (lease == null) {
                // Un altro pod e' arrivato prima, oppure la giornata e' ancora viva. Non e' un
                // errore: si passa alla successiva.
                continue;
            }
            try {
                retry(candidate, lease);
            } catch (FdrLeaseLostException e) {
                // Il possesso e' passato a un altro worker mentre lavoravamo. Lo stato della
                // giornata ora descrive il suo tentativo, non il nostro: non va toccato.
                logLeaseLost(candidate);
                continue;
            }
            retried++;
        }
        return retried;
    }

    /**
     * Le giornate scadute, una riga per giornata e non per dataset.
     *
     * <p>Il raggruppamento e' il punto: l'unita' di ripresa e' la <strong>giornata</strong>, perche'
     * e' l'unita' di elaborazione. Riprendere il singolo dataset lascerebbe scoperto il caso della
     * reingestion, dove FLUSSI e IUV sono inseparabili.
     *
     * <p>Il tetto sui tentativi e' qui oltre che nel core: cosi' una giornata esaurita non viene
     * nemmeno presa, invece di essere presa e subito scartata.
     */
    List<FdrRetryCandidate> findDue(Instant now) {
        return jdbcTemplate.query(
                "SELECT s.BATCH_ID, s.BUSINESS_DAY, b.WORKFLOW FROM " + schema
                        + ".FDR_INGESTION_STATUS s JOIN " + schema + ".FDR_INGESTION_BATCH b "
                        + "ON b.ID = s.BATCH_ID WHERE s.STATUS = ? AND s.NEXT_ATTEMPT_AT <= ? "
                        + "AND s.ATTEMPT_COUNT < ? "
                        + "GROUP BY s.BATCH_ID, s.BUSINESS_DAY, b.WORKFLOW "
                        + "ORDER BY s.BUSINESS_DAY, s.BATCH_ID",
                (rs, rowNum) -> new FdrRetryCandidate(rs.getLong("BATCH_ID"),
                        rs.getDate("BUSINESS_DAY").toLocalDate(),
                        FdrWorkflow.valueOf(rs.getString("WORKFLOW"))),
                FdrProcessingStatus.WAITING_RETRY.name(), Timestamp.from(now), maxAttempts);
    }

    /**
     * Le giornate rimaste in lavorazione il cui proprietario non rinnova piu' il lease.
     *
     * <p>Un lease <strong>nullo</strong> conta come scaduto, non come appena preso: significa che
     * nessuno puo' dimostrarne il possesso, e il claim non lascia mai quella combinazione — stato e
     * lease li scrive la stessa UPDATE. Trattarlo come "vivo" stranirebbe la giornata per sempre.
     *
     * <p>Il tetto sui tentativi vale anche qui: una giornata abbandonata che ha gia' esaurito i
     * cinque tentativi non va ripresa, altrimenti il poller la riprenderebbe a ogni giro senza mai
     * poterla chiudere.
     *
     * <p>{@code LIMIT} non e' paginazione — non c'e' una pagina due — e' un tetto al lavoro di un
     * giro: cio' che avanza viene ripreso fra cinque minuti. Senza, un incidente che lasciasse
     * indietro migliaia di giornate produrrebbe un singolo giro lunghissimo proprio mentre il
     * sistema e' in difficolta'.
     */
    List<FdrRetryCandidate> findAbandoned(Instant now) {
        return jdbcTemplate.query(
                "SELECT s.BATCH_ID, s.BUSINESS_DAY, b.WORKFLOW FROM " + schema
                        + ".FDR_INGESTION_STATUS s JOIN " + schema + ".FDR_INGESTION_BATCH b "
                        + "ON b.ID = s.BATCH_ID WHERE s.STATUS = ? "
                        + "AND (s.LEASE_EXPIRES_AT IS NULL OR s.LEASE_EXPIRES_AT <= ?) "
                        + "AND s.ATTEMPT_COUNT < ? "
                        + "GROUP BY s.BATCH_ID, s.BUSINESS_DAY, b.WORKFLOW "
                        + "ORDER BY s.BUSINESS_DAY, s.BATCH_ID LIMIT ?",
                (rs, rowNum) -> new FdrRetryCandidate(rs.getLong("BATCH_ID"),
                        rs.getDate("BUSINESS_DAY").toLocalDate(),
                        FdrWorkflow.valueOf(rs.getString("WORKFLOW"))),
                FdrProcessingStatus.PROCESSING.name(), Timestamp.from(now), maxAttempts,
                ABANDONED_SCAN_LIMIT);
    }

    /**
     * Ripete la giornata nella forma del suo workflow.
     *
     * <p>Per una reingestion questo significa <strong>rifare tutto</strong>: preflight, truncate
     * delle due partizioni, elaborazione. Non si riparte da meta': il tentativo precedente
     * potrebbe aver gia' svuotato la giornata e averne riscritto solo un pezzo, e senza un
     * checkpoint a livello di file non c'e' modo di sapere quale. Ricostruire e' l'unica
     * operazione di cui si conosce il risultato.
     */
    private void retry(FdrRetryCandidate candidate, FdrDayLease lease) {
        FdrLeaseGuard guard = ownership.guardFor(lease);
        if (candidate.workflow() == FdrWorkflow.REINGESTION
                && !prepareReingestion(candidate, lease, guard)) {
            return;
        }

        // REBUILD per la reingestione, che ha appena svuotato le due partizioni e deve riscriverle
        // entrambe; RESUME altrove, dove un dataset gia' concluso con successo va riportato e non
        // rielaborato - rielaborarlo ne raddoppierebbe le righe.
        FdrDayResult result = dayProcessor.process(lease,
                candidate.workflow() == FdrWorkflow.REINGESTION
                        ? FdrDayScope.REBUILD : FdrDayScope.RESUME);
        try (MdcScope ignored = MdcScope.phase("FDR_RETRY");
             LogDetails details = LogDetails.open()
                     .with(LogFields.EVENT_OUTCOME,
                             result.status() == FdrProcessingStatus.COMPLETED
                                     || result.status() == FdrProcessingStatus.COMPLETED_WITH_WARNING
                                     ? "success" : "failure")
                     .with("batch_id", candidate.batchId())
                     .with("business_day", candidate.businessDay().toString())
                     .with("workflow", candidate.workflow().name())
                     .with(LogFields.ATTEMPT, lease.attemptNo())
                     .with("day_status", result.status().name())
                     .with("rows_source", result.rowsSource())
                     .with("rows_processed", result.rowsProcessed())) {
            log.info("Retried an FDR day");
        }
    }

    /**
     * Prepara una reingestione ripresa, e <strong>non lascia mai la giornata appesa</strong>.
     *
     * <p>Qui stava un difetto della prima stesura. La giornata e' gia' stata reclamata
     * ({@code PROCESSING}) e nessun poller guardava quello stato per una REINGESTION. Qualunque
     * uscita da qui che non riscriva lo stato lasciava percio' la giornata ferma <em>per
     * sempre</em>, apparentemente in lavorazione. I due modi di uscire sono ora entrambi gestiti:
     *
     * <ul>
     *   <li><strong>sorgente non piu' ricostruibile</strong> - i file sono spariti o sono vuoti.
     *       E' un esito <em>terminale</em>, perche' rileggere lo stesso blob non ne cambia il
     *       contenuto: la giornata resta intatta e lo stato diventa {@code FAILED};</li>
     *   <li><strong>guasto tecnico</strong> (Azure irraggiungibile, database giu'): della sorgente
     *       non si sa ancora nulla, quindi la giornata torna in attesa <strong>consumando un
     *       tentativo</strong>. Consumarlo non e' un dettaglio: senza, un guasto persistente in
     *       fase di preparazione farebbe riprendere la stessa giornata ogni ora all'infinito.</li>
     * </ul>
     *
     * <p>Da quando il poller guarda anche le giornate abbandonate, un'uscita senza stato non
     * sarebbe piu' definitiva — verrebbe ripresa a lease scaduto — ma resta sbagliata: farebbe
     * aspettare cinque minuti e un tentativo in piu' per qualcosa di gia' noto.
     *
     * @return {@code true} se si puo' procedere con l'elaborazione
     */
    private boolean prepareReingestion(FdrRetryCandidate candidate, FdrDayLease lease,
                                       FdrLeaseGuard guard) {
        try {
            FdrReingestionPreparer.FdrPreflightResult preflight =
                    reingestionPreparer.preflight(candidate.businessDay());
            if (!preflight.rebuildable()) {
                statusRepository.settleDay(candidate.batchId(), candidate.businessDay(),
                        FdrProcessingStatus.PROCESSING, FdrProcessingStatus.FAILED,
                        preflight.reasonCode(), preflight.reason(), null, clock.instant());
                logRefusal(candidate, preflight.reasonCode(), false);
                return false;
            }
            reingestionPreparer.guardAndTruncate(candidate.businessDay(), guard);
            return true;
        } catch (FdrLeaseLostException e) {
            // Risale al chiamante senza scrivere nulla: la giornata non e' piu' nostra, e il suo
            // stato descrive ormai il lavoro di un altro worker.
            throw e;
        } catch (RuntimeException e) {
            boolean retryable = retryPolicy.hasAttemptsLeft(lease.attemptNo());
            statusRepository.settleDay(candidate.batchId(), candidate.businessDay(),
                    FdrProcessingStatus.PROCESSING,
                    retryable ? FdrProcessingStatus.WAITING_RETRY : FdrProcessingStatus.FAILED,
                    e.getClass().getSimpleName(), e.getMessage(),
                    retryable ? retryPolicy.nextAttemptAt(clock.instant()) : null, clock.instant());
            logRefusal(candidate, e.getClass().getSimpleName(), retryable);
            return false;
        }
    }

    private static void logLeaseLost(FdrRetryCandidate candidate) {
        try (MdcScope ignored = MdcScope.phase("FDR_RETRY");
             LogDetails details = LogDetails.open()
                     .with("batch_id", candidate.batchId())
                     .with("business_day", candidate.businessDay().toString())
                     .with("workflow", candidate.workflow().name())) {
            log.warn("Stopped an FDR retry: another worker took over the day");
        }
    }

    private static void logRefusal(FdrRetryCandidate candidate, String reason, boolean retryable) {
        try (MdcScope ignored = MdcScope.phase("FDR_RETRY");
             LogDetails details = LogDetails.open()
                     .with(LogFields.EVENT_OUTCOME, "failure")
                     .with("batch_id", candidate.batchId())
                     .with("business_day", candidate.businessDay().toString())
                     .with(LogFields.REASON, reason)
                     .with("will_retry", retryable)) {
            log.warn("Could not prepare the FDR day for reingestion");
        }
    }

    /** Una giornata pronta a essere ripresa. */
    public record FdrRetryCandidate(long batchId, LocalDate businessDay, FdrWorkflow workflow) {
    }
}
