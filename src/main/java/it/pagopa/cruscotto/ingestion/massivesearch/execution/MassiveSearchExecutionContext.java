package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Mutable state carried through a single Massive Search execution.
 *
 * <p>Populated by the engine as the pipeline advances (perimeter resolution, report generation,
 * ZIP creation) and read by the collaborators (report generators, ZIP assembler).</p>
 */
@Getter
@Setter
public class MassiveSearchExecutionContext {

    private final UUID instanceId;
    private final UUID executionId;
    private final String inputType;
    private final boolean rerun;

    /** Template of the input CSV (perimeter for FILTER, uploaded template for CSV). */
    private CsvTemplate inputTemplate;

    /** Storage path of the input CSV used to drive the analysis (legacy; may be null when DB-stored). */
    private String inputCsvPath;

    /** In-memory content of the input CSV (perimeter/uploaded) read from the DB; drives report generation. */
    private String inputCsvContent;

    /** Perimeter file id associated to this execution, when applicable. */
    private UUID perimeterFileId;

    /** Number of input rows (positions) to analyze. */
    private long totalInputRows;

    private long positionRows;
    private long tokenRows;
    private long transferRows;

    /**
     * Report types to produce for this execution (GUI checkbox selection). Defaults to all three so
     * an execution created without an explicit selection keeps the historical always-three behaviour.
     */
    private Set<ReportType> requestedReports = EnumSet.allOf(ReportType.class);

    /** Optional temporal window limiting the analysis; never {@code null}. */
    private AnalysisWindow analysisWindow = AnalysisWindow.none();

    /**
     * Timestamp shared by the result ZIP and the report CSVs it contains. Captured once at the start
     * of report generation so archive and CSV names carry the same timestamp token.
     */
    private LocalDateTime artifactTimestamp;

    /**
     * Diagnostica per report, aggiornata dal generatore a ogni batch.
     *
     * <p>Sta nel contesto e non nel valore di ritorno perche' il caso piu' interessante e' il
     * <strong>fallimento</strong>: su un report andato in timeout il metodo non ritorna nulla, ma le
     * metriche parziali (batch completati, chiavi processate, batch piu' lento) dicono dove si e'
     * fermato e con quale throughput. L'engine le rilegge da qui anche nel ramo di errore.</p>
     */
    private final Map<ReportType, StepMetrics> reportMetrics = new EnumMap<>(ReportType.class);

    /** Diagnostica della fase di perimetro, popolata da {@code resolveInput} e letta dall'engine. */
    private StepMetrics perimeterMetrics;

    /**
     * Battito di presenza, invocato dai collaboratori mentre avanzano.
     *
     * <p>Sta qui e non in un repository iniettato nei generatori di report per non far dipendere la
     * produzione di un CSV dalla tabella del ciclo di vita: il generatore sa di <em>avanzare</em>,
     * non sa che esista una riga {@code search_execution} da aggiornare. L'engine ci collega la
     * scrittura vera quando l'esecuzione passa a RUNNING.</p>
     *
     * <p>Il valore predefinito non fa nulla, quindi un contesto costruito in un test — o un
     * percorso che non lo collega — resta funzionante senza controlli sparsi ai punti di chiamata.</p>
     */
    private Runnable progressHeartbeat = () -> { };

    /**
     * Intervallo minimo fra due battiti scritti. Deve restare molto sotto
     * {@code running-timeout-minutes} (default 60 min), perche' e' il peggior ritardo con cui una
     * morte improvvisa viene notata, e molto sopra la durata di un batch, perche' e' quello che
     * rende gratuito chiamare {@link #heartbeat()} dentro il ciclo.
     */
    private static final long HEARTBEAT_MIN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

    /** Istante dell'ultimo battito scritto; {@code 0} finche' non ne e' stato scritto nessuno. */
    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private volatile long lastHeartbeatNanos;

    public MassiveSearchExecutionContext(UUID instanceId, UUID executionId, String inputType, boolean rerun) {
        this.instanceId = instanceId;
        this.executionId = executionId;
        this.inputType = inputType;
        this.rerun = rerun;
    }

    /** Registra (o sostituisce) la diagnostica corrente del report indicato. */
    public void putReportMetrics(ReportType type, StepMetrics metrics) {
        if (type != null && metrics != null) {
            reportMetrics.put(type, metrics);
        }
    }

    /** @return la diagnostica piu' recente del report, oppure {@code null} se non ne e' stata raccolta */
    public StepMetrics reportMetrics(ReportType type) {
        return type == null ? null : reportMetrics.get(type);
    }

    /**
     * Segnala che l'esecuzione sta avanzando.
     *
     * <p>Non deve mai poter interrompere il lavoro che sta misurando: un battito e' diagnostica, e
     * un errore nello scriverlo (connessione persa, lock) costerebbe l'intero report. Al massimo si
     * perde un battito, e se il problema persiste l'esecuzione verra' recuperata — che e' il
     * comportamento corretto, perche' a quel punto non sta davvero piu' avanzando.</p>
     */
    public void heartbeat() {
        Runnable callback = this.progressHeartbeat;
        if (callback == null) {
            return;
        }
        // Strozzatura a tempo, non a conteggio: i chiamanti battono a ogni unita' di lavoro e non
        // devono sapere quanto spesso convenga scrivere. Senza, un report con batch piccoli
        // produrrebbe un UPDATE ogni frazione di secondo per ore.
        long now = System.nanoTime();
        long last = lastHeartbeatNanos;
        if (last != 0L && now - last < HEARTBEAT_MIN_INTERVAL_NANOS) {
            return;
        }
        lastHeartbeatNanos = now;
        try {
            callback.run();
        } catch (RuntimeException ignored) {
            // best-effort: vedi javadoc
        }
    }
}
