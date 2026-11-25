package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * Riprende le giornate il cui tentativo successivo e' scaduto.
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

    private final JdbcTemplate jdbcTemplate;
    private final FdrDayProcessor dayProcessor;
    private final FdrReingestionPreparer reingestionPreparer;
    private final FdrRetryPolicy retryPolicy;
    private final FdrIngestionStatusRepository statusRepository;
    private final String schema;
    private final int maxAttempts;
    private final Clock clock;

    public FdrRetryService(JdbcTemplate jdbcTemplate, FdrDayProcessor dayProcessor,
                           FdrReingestionPreparer reingestionPreparer, FdrRetryPolicy retryPolicy,
                           FdrIngestionStatusRepository statusRepository,
                           DbSchemaConfig dbSchemaConfig, FdrProperties fdrProperties) {
        this(jdbcTemplate, dayProcessor, reingestionPreparer, retryPolicy, statusRepository,
                dbSchemaConfig, fdrProperties, Clock.systemUTC());
    }

    FdrRetryService(JdbcTemplate jdbcTemplate, FdrDayProcessor dayProcessor,
                    FdrReingestionPreparer reingestionPreparer, FdrRetryPolicy retryPolicy,
                    FdrIngestionStatusRepository statusRepository, DbSchemaConfig dbSchemaConfig,
                    FdrProperties fdrProperties, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.dayProcessor = dayProcessor;
        this.reingestionPreparer = reingestionPreparer;
        this.retryPolicy = retryPolicy;
        this.statusRepository = statusRepository;
        this.schema = dbSchemaConfig.getSchemaName();
        this.maxAttempts = fdrProperties.getRetry().getMaxAttempts();
        this.clock = clock;
    }

    /** @return quante giornate sono state riprese da questo worker */
    public int pollAndRetry() {
        Instant now = clock.instant();
        int retried = 0;
        for (FdrRetryCandidate candidate : findDue(now)) {
            if (!claim(candidate, now)) {
                continue;
            }
            retry(candidate);
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
     * Prende la giornata, tutti i suoi dataset insieme.
     *
     * <p>Stessa forma di claim dei command: una {@code UPDATE} condizionata sullo stato. Fra due
     * pod uno solo vede righe aggiornate. La condizione copre entrambi i dataset in un colpo, cosi'
     * non puo' capitare che un pod prenda i FLUSSI e un altro gli IUV della stessa giornata.
     */
    private boolean claim(FdrRetryCandidate candidate, Instant now) {
        int updated = jdbcTemplate.update(
                "UPDATE " + schema + ".FDR_INGESTION_STATUS SET STATUS = ?, UPDATED_AT = ?, "
                        + "NEXT_ATTEMPT_AT = NULL WHERE BATCH_ID = ? AND BUSINESS_DAY = ? "
                        + "AND STATUS = ?",
                FdrProcessingStatus.PROCESSING.name(), Timestamp.from(now), candidate.batchId(),
                Date.valueOf(candidate.businessDay()), FdrProcessingStatus.WAITING_RETRY.name());
        return updated > 0;
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
    private void retry(FdrRetryCandidate candidate) {
        if (candidate.workflow() == FdrWorkflow.REINGESTION && !prepareReingestion(candidate)) {
            return;
        }

        FdrDayResult result = dayProcessor.process(candidate.batchId(), candidate.businessDay());
        try (MdcScope ignored = MdcScope.phase("FDR_RETRY");
             LogDetails details = LogDetails.open()
                     .with(LogFields.EVENT_OUTCOME,
                             result.status() == FdrProcessingStatus.COMPLETED
                                     || result.status() == FdrProcessingStatus.COMPLETED_WITH_WARNING
                                     ? "success" : "failure")
                     .with("batch_id", candidate.batchId())
                     .with("business_day", candidate.businessDay().toString())
                     .with("workflow", candidate.workflow().name())
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
     * ({@code PROCESSING}) e nessun poller guarda quello stato per una REINGESTION: quello dei retry
     * cerca {@code WAITING_RETRY}, quello dei comandi cerca i batch {@code PENDING}. Qualunque
     * uscita da qui che non riscriva lo stato lascia percio' la giornata ferma <em>per sempre</em>,
     * apparentemente in lavorazione. I due modi di uscire sono ora entrambi gestiti:
     *
     * <ul>
     *   <li><strong>sorgente non piu' ricostruibile</strong> - i file sono spariti o sono vuoti.
     *       E' un esito <em>terminale</em>, perche' rileggere lo stesso blob non ne cambia il
     *       contenuto: la giornata resta intatta e lo stato diventa {@code FAILED};</li>
     *   <li><strong>guasto tecnico</strong> (Azure irraggiungibile, database giu'): della sorgente
     *       non si sa ancora nulla, quindi la giornata torna in attesa <strong>consumando un
     *       tentativo</strong>. Consumarlo non e' un dettaglio: {@code ATTEMPT_COUNT} cresce dentro
     *       l'elaborazione, che qui non viene raggiunta, quindi senza questo incremento un guasto
     *       persistente in fase di preparazione farebbe riprendere la stessa giornata ogni ora
     *       all'infinito.</li>
     * </ul>
     *
     * @return {@code true} se si puo' procedere con l'elaborazione
     */
    private boolean prepareReingestion(FdrRetryCandidate candidate) {
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
            reingestionPreparer.guardAndTruncate(candidate.businessDay());
            return true;
        } catch (RuntimeException e) {
            boolean retryable = retryPolicy.hasAttemptsLeft(attemptsOf(candidate));
            statusRepository.settleDay(candidate.batchId(), candidate.businessDay(),
                    FdrProcessingStatus.PROCESSING,
                    retryable ? FdrProcessingStatus.WAITING_RETRY : FdrProcessingStatus.FAILED,
                    e.getClass().getSimpleName(), e.getMessage(),
                    retryable ? retryPolicy.nextAttemptAt(clock.instant()) : null, clock.instant());
            logRefusal(candidate, e.getClass().getSimpleName(), retryable);
            return false;
        }
    }

    /**
     * I tentativi già consumati dalla giornata: il massimo fra i suoi dataset.
     *
     * <p>Il massimo e non la somma: i due dataset vengono tentati insieme, quindi condividono il
     * conteggio. Sommarli dimezzerebbe i tentativi disponibili.
     */
    private int attemptsOf(FdrRetryCandidate candidate) {
        Integer attempts = jdbcTemplate.queryForObject(
                "SELECT max(ATTEMPT_COUNT) FROM " + schema + ".FDR_INGESTION_STATUS "
                        + "WHERE BATCH_ID = ? AND BUSINESS_DAY = ?",
                Integer.class, candidate.batchId(), Date.valueOf(candidate.businessDay()));
        return attempts == null ? 0 : attempts;
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
