package it.pagopa.cruscotto.ingestion.massivesearch.report;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvLineWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.PerimeterCsvReader;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.AnalysisWindow;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionContext;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.StepMetrics;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.MassiveSearchExecutionException;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.SearchReportGenerator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Shared skeleton for the three Massive Search report generators. Owns the common pipeline — write the
 * header, stream the (de-duplicated, batched) perimeter keys through {@link PerimeterCsvReader}, run one
 * set-based query per batch and stream the resulting rows to the report writer — leaving each concrete
 * generator to declare only its {@link #type()}, {@link #headers()} and the batch query
 * ({@link #streamByKeys}). This removes the previously triplicated read/dedup/batch loop.
 *
 * @param <R> the concrete report row type produced by this generator
 */
@Slf4j
public abstract class AbstractPerimeterReportGenerator<R extends ReportRow> implements SearchReportGenerator {

    private final PerimeterCsvReader perimeterReader;
    private final CsvLineWriter lineWriter;
    private final int batchSize;

    /**
     * Tetto al numero di chiavi per statement. Ogni chiave occupa 1-2 parametri bind (2 per i
     * template NAV_PA e IUV_PA) e PostgreSQL ne ammette al massimo 65535 per prepared statement:
     * oltre questa soglia PgJDBC fallirebbe a runtime, su un report gia' avviato. Il valore e'
     * volutamente sotto il limite teorico (32767) per lasciare spazio ai parametri della finestra.
     */
    private static final int MAX_PERIMETER_BATCH_SIZE = 30_000;

    /** Ogni quanti batch emettere {@code REPORT_PROGRESS}. */
    private static final int PROGRESS_EVERY_BATCHES = 50;

    protected AbstractPerimeterReportGenerator(
        PerimeterCsvReader perimeterReader,
        CsvLineWriter lineWriter,
        MassiveSearchProperties properties
    ) {
        this.perimeterReader = perimeterReader;
        this.lineWriter = lineWriter;
        int configured = Math.max(1, properties.getExecution().getPerimeterBatchSize());
        if (configured > MAX_PERIMETER_BATCH_SIZE) {
            try (MdcScope ignored = MdcScope.phase("CONFIG_CLAMP");
                 LogDetails details = LogDetails.open()
                         .with("property", "massive-search.execution.perimeter-batch-size")
                         .with("configured", configured)
                         .with("applied", MAX_PERIMETER_BATCH_SIZE)
                         .with(LogFields.REASON, "postgres-bind-parameter-limit")) {
                log.warn("Clamped a configured value to its safe maximum");
            }
            configured = MAX_PERIMETER_BATCH_SIZE;
        }
        this.batchSize = configured;
    }

    @Override
    public long writeReport(MassiveSearchExecutionContext context, Writer writer) throws IOException {
        String content = context.getInputCsvContent();
        if (content == null) {
            throw new MassiveSearchExecutionException(
                "Missing input CSV content for " + type() + " report, instanceId=" + context.getInstanceId());
        }

        lineWriter.writeLine(writer, headers());

        AnalysisWindow window = context.getAnalysisWindow();
        long startedNanos = System.nanoTime();
        AtomicLong batches = new AtomicLong();
        AtomicLong keys = new AtomicLong();
        AtomicLong rowsSoFar = new AtomicLong();
        AtomicLong slowestBatchMs = new AtomicLong();
        // Seminata PRIMA del loop. publishMetrics gira dopo che streamByKeys e' tornata, quindi un
        // fallimento sul PRIMO batch non lascerebbe alcuna diagnostica sullo step — ed e' il caso piu'
        // frequente, perche' una query troppo lenta lo e' subito. Con il seme lo step dice almeno su
        // quale template e con quale dimensione di batch si e' fermato, che sono le due informazioni
        // da cui si riparte.
        publishMetrics(context, 0L, 0L, 0L, 0L, startedNanos);
        long rows = perimeterReader.forEachBatch(content, context.getInputTemplate(), batchSize,
            (template, batch) -> {
                long batchStartedNanos = System.nanoTime();
                long produced = streamByKeys(template, batch, window, row -> writeRowUnchecked(writer, row));
                long batchMs = elapsedMs(batchStartedNanos);
                slowestBatchMs.accumulateAndGet(batchMs, Math::max);
                long doneBatches = batches.incrementAndGet();
                long doneKeys = keys.addAndGet(batch.size());
                rowsSoFar.addAndGet(produced);
                // Aggiornata a ogni batch, non solo alla fine: se il report va in timeout il valore di
                // ritorno non arriva mai, ma l'engine rilegge queste metriche parziali dal contesto.
                publishMetrics(context, doneBatches, doneKeys, rowsSoFar.get(), slowestBatchMs.get(), startedNanos);
                logProgress(context, doneBatches, doneKeys, startedNanos);
                // A OGNI batch, non alla cadenza del log di avanzamento: il battito dice al recoverer
                // che questa esecuzione e' viva, e legarlo a un conteggio di batch lo renderebbe
                // dipendente dalla dimensione del batch — con batch grandi e lenti la soglia potrebbe
                // scadere fra un battito e il successivo. La strozzatura a tempo e' nel contesto.
                context.heartbeat();
                return produced;
            });

        long elapsedMs = elapsedMs(startedNanos);
        publishMetrics(context, batches.get(), keys.get(), rows, slowestBatchMs.get(), startedNanos);
        try (MdcScope ignored = MdcScope.phase("REPORT_GENERATED");
             LogDetails details = LogDetails.open()
                     .with("report", type())
                     .with(LogFields.ROWS, rows)
                     .with("keys", keys.get())
                     .with("batches", batches.get())
                     .with(LogFields.ELAPSED_MS, elapsedMs)
                     .with(LogFields.FROM, window.fromInclusive())
                     .with(LogFields.TO, window.toExclusive())) {
            log.info("Generated report");
        }
        if (rows == 0 && window.hasBounds()) {
            // Causa piu' frequente di report vuoto: la finestra non seleziona nulla del perimetro.
            try (MdcScope ignored = MdcScope.phase("REPORT_EMPTY");
                 LogDetails details = LogDetails.open()
                         .with("report", type())
                         .with(LogFields.FROM, window.fromInclusive())
                         .with(LogFields.TO, window.toExclusive())) {
                log.warn("The analysis window selects no data: check the requested period and the "
                        + "default lookback against the age of the data actually present");
            }
        }
        return rows;
    }

    /**
     * Avanzamento periodico del loop di batching.
     *
     * <p>Un report su un perimetro grande e' una sequenza di centinaia di query che puo' durare
     * decine di minuti: senza questa traccia, fra l'inizio e {@code REPORT_GENERATED} non viene
     * emesso nulla e in caso di rallentamento o di timeout non c'e' modo di sapere a che punto fosse
     * arrivato, ne' di stimare il throughput per tarare {@code running-timeout-minutes}.</p>
     */
    private void logProgress(MassiveSearchExecutionContext context, long batches, long keys, long startedNanos) {
        if (batches % PROGRESS_EVERY_BATCHES != 0) {
            return;
        }
        long elapsedMs = elapsedMs(startedNanos);
        try (MdcScope ignored = MdcScope.phase("REPORT_PROGRESS");
             LogDetails details = LogDetails.open()
                     .with("report", type())
                     .with("batches", batches)
                     .with("keys", keys)
                     .with(LogFields.ELAPSED_MS, elapsedMs)
                     .with("keys_per_sec", elapsedMs > 0 ? (keys * 1000L / elapsedMs) : 0L)) {
            log.info("Report batching in progress");
        }
    }

    /**
     * Diagnostica del report, destinata a {@code search_execution_step.metrics}.
     *
     * <p>{@code slowestBatchMs} e' il valore che serve davvero a tarare la configurazione: il
     * {@code statement-timeout} e il {@code socketTimeout} si applicano al <em>singolo</em> statement,
     * quindi e' il batch piu' lento — non la durata totale — a dire quanto siamo vicini al limite.
     * {@code batchSize} e {@code childMarginDays} sono registrati perche' sono le due leve con cui si
     * interviene, e senza di essi i numeri di esecuzioni diverse non sono confrontabili.</p>
     */
    private void publishMetrics(MassiveSearchExecutionContext context, long batches, long keys, long rows,
                                long slowestBatchMs, long startedNanos) {
        long elapsedMs = elapsedMs(startedNanos);
        context.putReportMetrics(type(), StepMetrics.create()
            // Il template determina la forma del join sulle chiavi, quindi quale query e' stata
            // eseguita: lo step non lo riporta altrove, e senza di esso due fallimenti su perimetri
            // diversi sono indistinguibili.
            .with("template", context.getInputTemplate())
            .with("keys", keys)
            .with("batches", batches)
            .with("rows", rows)
            .with("batchSize", batchSize)
            .with("slowestBatchMs", slowestBatchMs)
            .with("elapsedMs", elapsedMs)
            .with("keysPerSec", elapsedMs > 0 ? (keys * 1000L / elapsedMs) : 0L));
    }

    private static long elapsedMs(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }

    private void writeRowUnchecked(Writer writer, R row) {
        try {
            lineWriter.writeLine(writer, row.values());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Ordered header names of this report. */
    protected abstract List<String> headers();

    /**
     * Streams the report rows matching the given batch of keys within the window, invoking the consumer
     * for each produced row and returning the number of rows produced.
     */
    protected abstract long streamByKeys(CsvTemplate template, List<SearchInputRow> keys,
                                         AnalysisWindow window, Consumer<R> consumer);
}
