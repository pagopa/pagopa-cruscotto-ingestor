package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReadResult;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileReader;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrRowMapper;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrRowWriter;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteResult;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrWriteSession;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobContentReader;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobDiscoveryService;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDayDiscovery;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrInvalidEntry;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Elabora UNA giornata FDR: i due dataset, insieme.
 *
 * <p>È il pezzo che DAILY, BOOTSTRAP, REINGESTION e il retry useranno tutti e quattro, ed è per
 * questo che non sa nulla di loro: riceve una giornata e un batch, e non consulta mai l'orologio
 * per decidere <em>quale</em> giornata — quella scelta appartiene a chi lo invoca, e metterla qui
 * renderebbe BOOTSTRAP impossibile e la REINGESTION di una giornata storica un incidente.
 *
 * <p><strong>La giornata è l'unità.</strong> FLUSSI e IUV si elaborano insieme e il loro esito si
 * compone: {@code FDR_INGESTION_STATUS.DATASET} esiste per lo stato e i contatori, non per
 * permettere di chiedere un dataset solo.
 *
 * <p><strong>Dove si può rompere, e cosa resta.</strong> Le righe business sono scritte in
 * transazioni per batch, lo stato operativo in scritture separate: se il processo muore a metà,
 * restano righe scritte e uno status fermo su {@code PROCESSING}. È voluto — l'alternativa è
 * un'unica transazione per milioni di righe — ed è recuperabile: una giornata in
 * {@code PROCESSING} non è terminale, quindi la guardia di idempotenza la lascia rielaborare, e la
 * REINGESTION (che svuota la giornata prima di riscriverla) è il percorso che rimette le cose a
 * posto senza duplicare. Quale componente rilevi gli status rimasti appesi è una decisione dello
 * scheduler, non di qui.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrDayProcessor {

    private final FdrBlobDiscoveryService discoveryService;
    private final FdrBlobContentReader contentReader;
    private final FdrFileReader fileReader;
    private final FdrRowWriter rowWriter;
    private final FdrIngestionStatusRepository statusRepository;
    private final FdrIngestionEventRepository eventRepository;
    private final FdrRowErrorStaging rowErrorStaging;
    private final FdrRetryPolicy retryPolicy;
    private final Clock clock;

    public FdrDayProcessor(FdrBlobDiscoveryService discoveryService,
                           FdrBlobContentReader contentReader,
                           FdrFileReader fileReader,
                           FdrRowWriter rowWriter,
                           FdrIngestionStatusRepository statusRepository,
                           FdrIngestionEventRepository eventRepository,
                           FdrRowErrorStaging rowErrorStaging,
                           FdrRetryPolicy retryPolicy) {
        this(discoveryService, contentReader, fileReader, rowWriter, statusRepository,
                eventRepository, rowErrorStaging, retryPolicy, Clock.systemUTC());
    }

    /**
     * Seam per i test. L'orologio serve solo ai timestamp operativi — mai ai valori business, che
     * vengono dal file e dalla cartella.
     */
    FdrDayProcessor(FdrBlobDiscoveryService discoveryService, FdrBlobContentReader contentReader,
                    FdrFileReader fileReader, FdrRowWriter rowWriter,
                    FdrIngestionStatusRepository statusRepository,
                    FdrIngestionEventRepository eventRepository, FdrRowErrorStaging rowErrorStaging,
                    FdrRetryPolicy retryPolicy, Clock clock) {
        this.discoveryService = discoveryService;
        this.contentReader = contentReader;
        this.fileReader = fileReader;
        this.rowWriter = rowWriter;
        this.statusRepository = statusRepository;
        this.eventRepository = eventRepository;
        this.rowErrorStaging = rowErrorStaging;
        this.retryPolicy = retryPolicy;
        this.clock = clock;
    }

    /**
     * @param batchId     la richiesta a cui la giornata appartiene
     * @param businessDay la giornata FDR, cioè la cartella {@code yyyyMMdd} del blob
     */
    public FdrDayResult process(long batchId, LocalDate businessDay) {
        Objects.requireNonNull(businessDay, "businessDay");
        Instant now = clock.instant();
        String runId = "fdr-" + batchId + "-" + businessDay;

        Map<FdrDataset, FdrDatasetResult> settled = alreadySettled(batchId, businessDay);
        if (settled != null) {
            FdrDayResult skipped = new FdrDayResult(batchId, businessDay,
                    aggregate(settled.values()), settled, true);
            logDay(skipped);
            return skipped;
        }

        Map<FdrDataset, FdrIngestionStatusRepository.FdrStatusRow> statuses =
                new EnumMap<>(FdrDataset.class);
        for (FdrDataset dataset : FdrDataset.values()) {
            statuses.put(dataset, statusRepository.markProcessing(batchId, businessDay, dataset, now));
        }

        FdrDayDiscovery discovery;
        try {
            discovery = discoveryService.discover(businessDay);
        } catch (RuntimeException e) {
            // La discovery e' di giornata: se fallisce, nessuno dei due dataset e' valutabile.
            return finishAll(batchId, businessDay, statuses, technicalResult(e), now, e);
        }
        recordDiscoveryAnomalies(batchId, businessDay, discovery, now);

        Map<FdrDataset, FdrDatasetResult> results = new EnumMap<>(FdrDataset.class);
        for (FdrDataset dataset : FdrDataset.values()) {
            FdrIngestionStatusRepository.FdrStatusRow status = statuses.get(dataset);
            FdrDatasetResult result = processDataset(batchId, businessDay, dataset,
                    discovery.filesOf(dataset), status.id(), runId, now);
            result = applyRetryBookkeeping(batchId, businessDay, result, status, now);
            results.put(dataset, result);
        }

        FdrDayResult day = new FdrDayResult(batchId, businessDay, aggregate(results.values()),
                results, false);
        logDay(day);
        return day;
    }

    /**
     * Le entry del blob non elaborabili, riassunte <strong>per ragione</strong> e in un evento solo.
     *
     * <p>I conteggi per ragione sono limitati dalla dimensione dell'enum, quindi la riga non può
     * crescere a sorpresa; i nomi dei file restano nel log di discovery, a DEBUG. Un evento per
     * file anomalo trasformerebbe una cartella sporca in centinaia di righe di audit che dicono
     * tutte la stessa cosa.
     */
    private void recordDiscoveryAnomalies(long batchId, LocalDate businessDay,
                                          FdrDayDiscovery discovery, Instant now) {
        List<FdrInvalidEntry> invalid = discovery.invalidEntries();
        if (invalid.isEmpty()) {
            return;
        }
        Map<FdrInvalidEntry.Reason, Long> byReason = new EnumMap<>(FdrInvalidEntry.Reason.class);
        invalid.forEach(entry -> byReason.merge(entry.reason(), 1L, Long::sum));

        eventRepository.record(batchId, null, businessDay, null, null,
                FdrEventType.DISCOVERY_ANOMALIES, FdrEventType.WARNING, "DISCOVERY_ANOMALIES",
                "Blob entries that cannot be processed", "entries=" + invalid.size()
                        + " " + byReason, now);
    }

    // ─── Guardia di idempotenza ──────────────────────────────────────────────────

    /**
     * Una giornata già chiusa con successo non si rielabora.
     *
     * <p>Rielaborarla raddoppierebbe le righe: la scrittura è un INSERT fedele, senza deduplica e
     * senza upsert, perché i duplicati del sorgente devono arrivare. Sovrascrivere una giornata è
     * una cosa sola — la REINGESTION, che la svuota prima — e deve essere chiesta esplicitamente.
     *
     * <p>{@code PROCESSING}, {@code FAILED}, {@code MISSING} e {@code WAITING_RETRY} non fermano
     * nulla: sono esattamente gli stati da cui si riprende.
     *
     * @return gli esiti registrati se la giornata è chiusa, {@code null} se va elaborata
     */
    private Map<FdrDataset, FdrDatasetResult> alreadySettled(long batchId, LocalDate businessDay) {
        Map<FdrDataset, FdrDatasetResult> settled = new EnumMap<>(FdrDataset.class);
        for (FdrDataset dataset : FdrDataset.values()) {
            var existing = statusRepository.find(batchId, businessDay, dataset);
            if (existing.isEmpty() || !existing.get().status().isTerminalSuccess()) {
                return null;
            }
            // I contatori non vengono riletti: stanno gia' in colonna, e riportarli qui a zero
            // sarebbe meno ambiguo che riportarli a meta'. Il flag skipped dice come leggerli.
            settled.put(dataset, FdrDatasetResult.of(dataset, existing.get().status(), 0, 0,
                    null, null));
        }
        return settled;
    }

    // ─── Un dataset ──────────────────────────────────────────────────────────────

    private FdrDatasetResult processDataset(long batchId, LocalDate businessDay,
                                            FdrDataset dataset, List<FdrBlobFile> files,
                                            long statusId, String runId, Instant now) {
        if (files.isEmpty()) {
            eventRepository.record(batchId, statusId, businessDay, dataset, null,
                    FdrEventType.DATASET_MISSING, FdrEventType.WARNING, "DATASET_MISSING",
                    "No file found for the dataset after a successful discovery", null, now);
            return FdrDatasetResult.of(dataset, FdrProcessingStatus.MISSING, 0, 0,
                    "DATASET_MISSING", "no file published for this day");
        }

        DatasetTotals totals = new DatasetTotals();
        try {
            for (FdrBlobFile file : files) {
                processFile(batchId, businessDay, dataset, file, statusId, runId, now, totals);
            }
        } catch (IOException | RuntimeException e) {
            return failedOrRetryable(batchId, businessDay, dataset, statusId, now, totals, e);
        }

        recordTruncation(batchId, businessDay, dataset, statusId, now, totals);
        return outcomeOf(batchId, businessDay, dataset, statusId, now, totals);
    }

    private void processFile(long batchId, LocalDate businessDay, FdrDataset dataset,
                             FdrBlobFile file, long statusId, String runId, Instant now,
                             DatasetTotals totals) throws IOException {
        // Lo stream e' chiuso anche dal lettore: chiuderlo due volte non costa nulla, mentre non
        // chiuderlo quando l'apertura della sessione fallisce lascerebbe appesa una connessione
        // HTTP per ogni file.
        try (InputStream content = contentReader.open(file)) {
            FdrWriteSession session = rowWriter.openSession(file);
            FdrFileReadResult read = fileReader.read(content, file,
                    FdrRowMapper.forDataset(dataset), session::accept);
            FdrWriteResult written = session.complete();

            totals.rowsSource += read.dataRows();
            totals.rowsProcessed += written.rowsInserted();
            totals.truncatedRows += written.truncation().affectedRows();
            totals.maxTruncatedLength = Math.max(totals.maxTruncatedLength,
                    written.truncation().maxSourceLength());

            if (read.isEmptySource()) {
                totals.emptyFiles++;
                eventRepository.record(batchId, statusId, businessDay, dataset, file.blobName(),
                        FdrEventType.EMPTY_SOURCE_FILE, FdrEventType.WARNING, "EMPTY_SOURCE_FILE",
                        "Valid file with a header and no data rows", null, now);
            }

            long discarded = read.rejectedRows() + written.rowsRejected();
            if (discarded > 0) {
                rowErrorStaging.stage(runId, file, read.rejectionSample(), written.errorSample());
                eventRepository.record(batchId, statusId, businessDay, dataset, file.blobName(),
                        FdrEventType.ROWS_DISCARDED, FdrEventType.WARNING, "ROWS_DISCARDED",
                        "Rows that did not reach the database",
                        "parse=" + read.rejectedRows() + " persistence=" + written.rowsRejected()
                                + " of " + read.dataRows(), now);
            }
        }
    }

    /**
     * Il troncamento autorizzato, aggregato per dataset e giornata.
     *
     * <p>Un evento, non 40.570. È il numero di una giornata reale, e scriverne uno per riga
     * riempirebbe la tabella di audit con la copia di un dato che è già in colonna.
     */
    private void recordTruncation(long batchId, LocalDate businessDay, FdrDataset dataset,
                                  long statusId, Instant now, DatasetTotals totals) {
        if (totals.truncatedRows == 0) {
            return;
        }
        eventRepository.record(batchId, statusId, businessDay, dataset, null,
                FdrEventType.SOURCE_VALUE_TRUNCATED, FdrEventType.WARNING,
                "SOURCE_VALUE_TRUNCATED", "Source values shortened to the column width",
                "field=CAUSALE affected_rows=" + totals.truncatedRows
                        + " max_source_len=" + totals.maxTruncatedLength + " persisted_len=30",
                now);
    }

    private FdrDatasetResult outcomeOf(long batchId, LocalDate businessDay, FdrDataset dataset,
                                       long statusId, Instant now, DatasetTotals totals) {
        if (totals.rowsSource == 0) {
            // File presenti ma zero record: la sorgente e' stata letta correttamente, quindi
            // rileggerla non cambierebbe nulla. Terminale, non ritentabile.
            eventRepository.record(batchId, statusId, businessDay, dataset, null,
                    FdrEventType.EMPTY_DATASET, FdrEventType.ERROR, "EMPTY_DATASET",
                    "Files present but no data row in the whole dataset",
                    "files=" + totals.emptyFiles, now);
            return FdrDatasetResult.of(dataset, FdrProcessingStatus.FAILED, 0, 0, "EMPTY_DATASET",
                    "no data row in any file of the dataset");
        }
        if (totals.rowsProcessed == 0) {
            return FdrDatasetResult.of(dataset, FdrProcessingStatus.FAILED, totals.rowsSource, 0,
                    "ALL_ROWS_FAILED", "no row of the dataset reached the database");
        }
        if (totals.rowsProcessed == totals.rowsSource && !totals.hasWarnings()) {
            return FdrDatasetResult.of(dataset, FdrProcessingStatus.COMPLETED, totals.rowsSource,
                    totals.rowsProcessed, null, null);
        }
        return FdrDatasetResult.of(dataset, FdrProcessingStatus.COMPLETED_WITH_WARNING,
                totals.rowsSource, totals.rowsProcessed, warningCode(totals),
                warningMessage(totals));
    }

    private static String warningCode(DatasetTotals totals) {
        if (totals.rowsProcessed < totals.rowsSource) {
            return "ROWS_DISCARDED";
        }
        return totals.truncatedRows > 0 ? "SOURCE_VALUE_TRUNCATED" : "EMPTY_SOURCE_FILE";
    }

    private static String warningMessage(DatasetTotals totals) {
        return "x=" + totals.rowsSource + " n=" + totals.rowsProcessed
                + " truncated=" + totals.truncatedRows + " empty_files=" + totals.emptyFiles;
    }

    // ─── Guasti ──────────────────────────────────────────────────────────────────

    private FdrDatasetResult failedOrRetryable(long batchId, LocalDate businessDay,
                                               FdrDataset dataset, long statusId, Instant now,
                                               DatasetTotals totals, Exception failure) {
        boolean retryable = retryPolicy.isRetryable(failure);
        String eventType = retryable ? FdrEventType.TECHNICAL_ERROR
                : FdrEventType.FILE_STRUCTURAL_ERROR;
        eventRepository.record(batchId, statusId, businessDay, dataset, null, eventType,
                FdrEventType.ERROR, failure.getClass().getSimpleName(),
                "Dataset processing stopped", failure.getMessage(), now);

        FdrProcessingStatus status = retryable ? FdrProcessingStatus.WAITING_RETRY
                : FdrProcessingStatus.FAILED;
        return FdrDatasetResult.of(dataset, status, totals.rowsSource, totals.rowsProcessed,
                failure.getClass().getSimpleName(), failure.getMessage());
    }

    private Map<FdrDataset, FdrDatasetResult> technicalResult(RuntimeException failure) {
        FdrProcessingStatus status = retryPolicy.isRetryable(failure)
                ? FdrProcessingStatus.WAITING_RETRY : FdrProcessingStatus.FAILED;
        Map<FdrDataset, FdrDatasetResult> results = new EnumMap<>(FdrDataset.class);
        for (FdrDataset dataset : FdrDataset.values()) {
            results.put(dataset, FdrDatasetResult.of(dataset, status, 0, 0,
                    failure.getClass().getSimpleName(), failure.getMessage()));
        }
        return results;
    }

    private FdrDayResult finishAll(long batchId, LocalDate businessDay,
                                   Map<FdrDataset, FdrIngestionStatusRepository.FdrStatusRow> statuses,
                                   Map<FdrDataset, FdrDatasetResult> results, Instant now,
                                   RuntimeException failure) {
        Map<FdrDataset, FdrDatasetResult> finished = new EnumMap<>(FdrDataset.class);
        for (FdrDataset dataset : FdrDataset.values()) {
            FdrIngestionStatusRepository.FdrStatusRow status = statuses.get(dataset);
            eventRepository.record(batchId, status.id(), businessDay, dataset, null,
                    FdrEventType.TECHNICAL_ERROR, FdrEventType.ERROR,
                    failure.getClass().getSimpleName(), "Discovery failed for the day",
                    failure.getMessage(), now);
            finished.put(dataset, applyRetryBookkeeping(batchId, businessDay,
                    results.get(dataset), status, now));
        }
        FdrDayResult day = new FdrDayResult(batchId, businessDay, aggregate(finished.values()),
                finished, false);
        logDay(day);
        return day;
    }

    // ─── Retry e scrittura dello stato ───────────────────────────────────────────

    /**
     * Scrive l'esito e, se è un guasto transitorio, programma il tentativo successivo.
     *
     * <p>Quando i tentativi finiscono l'esito diventa {@code FAILED} e {@code NEXT_ATTEMPT_AT}
     * resta vuoto: un poller che trovasse una data lì dentro riprenderebbe per sempre una giornata
     * che nessuno ha più intenzione di ritentare.
     */
    private FdrDatasetResult applyRetryBookkeeping(long batchId, LocalDate businessDay,
                                                   FdrDatasetResult result,
                                                   FdrIngestionStatusRepository.FdrStatusRow status,
                                                   Instant now) {
        FdrDatasetResult finalResult = result;
        Instant nextAttemptAt = null;

        if (result.status() == FdrProcessingStatus.WAITING_RETRY) {
            if (retryPolicy.hasAttemptsLeft(status.attemptCount())) {
                nextAttemptAt = retryPolicy.nextAttemptAt(now);
                eventRepository.record(batchId, status.id(), businessDay, result.dataset(), null,
                        FdrEventType.RETRY_SCHEDULED, FdrEventType.WARNING, "RETRY_SCHEDULED",
                        "Another attempt was scheduled",
                        "attempt=" + status.attemptCount() + "/" + retryPolicy.maxAttempts(), now);
            } else {
                finalResult = FdrDatasetResult.of(result.dataset(), FdrProcessingStatus.FAILED,
                        result.rowsSource(), result.rowsProcessed(), "RETRY_EXHAUSTED",
                        result.errorMessage());
                eventRepository.record(batchId, status.id(), businessDay, result.dataset(), null,
                        FdrEventType.RETRY_EXHAUSTED, FdrEventType.ERROR, "RETRY_EXHAUSTED",
                        "No attempt left for this day",
                        "attempts=" + status.attemptCount() + "/" + retryPolicy.maxAttempts(), now);
            }
        }

        statusRepository.complete(status.id(), finalResult, now, nextAttemptAt);
        return finalResult;
    }

    // ─── Aggregazione di giornata ────────────────────────────────────────────────

    /**
     * Lo stato della giornata a partire da quello dei due dataset.
     *
     * <p>{@code WAITING_RETRY} vince su tutto, e non è una scelta di comodo: se un dataset non è
     * stato elaborato per un guasto, la giornata non <em>ha ancora</em> un esito, e dichiararla
     * fallita o completata sarebbe inventarselo. Poi vengono gli esiti terminali negativi —
     * {@code FAILED} e {@code MISSING}, che per la giornata pesano uguale perché in entrambi i
     * casi metà del dato non c'è — e solo se nulla è andato storto si guarda ai warning.
     */
    private static FdrProcessingStatus aggregate(java.util.Collection<FdrDatasetResult> results) {
        if (results.stream().anyMatch(r -> r.status() == FdrProcessingStatus.WAITING_RETRY)) {
            return FdrProcessingStatus.WAITING_RETRY;
        }
        if (results.stream().anyMatch(r -> r.status() == FdrProcessingStatus.FAILED
                || r.status() == FdrProcessingStatus.MISSING)) {
            return FdrProcessingStatus.FAILED;
        }
        if (results.stream().anyMatch(r -> r.status() == FdrProcessingStatus.COMPLETED_WITH_WARNING)) {
            return FdrProcessingStatus.COMPLETED_WITH_WARNING;
        }
        return FdrProcessingStatus.COMPLETED;
    }

    /** Una riga per giornata, con i due numeri su cui si decide e lo stato che ne è uscito. */
    private static void logDay(FdrDayResult day) {
        try (MdcScope ignored = MdcScope.phase("FDR_DAY");
             LogDetails details = LogDetails.open()
                     .with(LogFields.EVENT_OUTCOME,
                             day.status() == FdrProcessingStatus.COMPLETED
                                     || day.status() == FdrProcessingStatus.COMPLETED_WITH_WARNING
                                     ? "success" : "failure")
                     .with("business_day", day.businessDay().toString())
                     .with("batch_id", day.batchId())
                     .with("day_status", day.status().name())
                     .with("rows_source", day.rowsSource())
                     .with("rows_processed", day.rowsProcessed())
                     .with("skipped", day.skipped())) {
            day.datasets().forEach((dataset, result) ->
                    details.with(dataset.name().toLowerCase(java.util.Locale.ROOT) + "_status",
                            result.status().name()));
            log.info("Processed FDR day");
        }
    }

    /** I totali di un dataset mentre si accumulano. Mutabile e locale a una chiamata. */
    private static final class DatasetTotals {
        private long rowsSource;
        private long rowsProcessed;
        private long truncatedRows;
        private int maxTruncatedLength;
        private int emptyFiles;

        boolean hasWarnings() {
            return truncatedRows > 0 || emptyFiles > 0;
        }
    }
}
