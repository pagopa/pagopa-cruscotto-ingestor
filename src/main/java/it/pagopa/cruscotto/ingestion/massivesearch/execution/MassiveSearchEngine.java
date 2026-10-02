package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.naming.MassiveSearchArtifactNaming;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterCsvGenerator;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterFileMetadata;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterFileRepository;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterGenerationResult;
import it.pagopa.cruscotto.ingestion.massivesearch.storage.MassiveSearchStorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Massive Search execution pipeline: resolves the input perimeter, generates the three per-execution
 * reports, assembles the result ZIP and returns the aggregated outcome. Persistence of execution and
 * instance state is handled by {@link MassiveSearchExecutionService}.
 */
@Slf4j
@Service
public class MassiveSearchEngine {

    static final String INPUT_TYPE_FILTER = "FILTER";
    static final String INPUT_TYPE_CSV = "CSV";

    private final MassiveSearchProperties properties;
    private final PerimeterCsvGenerator perimeterGenerator;
    private final PerimeterFileRepository perimeterFileRepository;
    private final AnalysisWindowResolver analysisWindowResolver;
    private final MassiveSearchStorageService storage;
    private final ResultZipService resultZipService;
    private final SearchExecutionStepRepository stepRepository;
    private final MassiveSearchArtifactNaming naming;
    private final Map<ReportType, SearchReportGenerator> reportGenerators;

    public MassiveSearchEngine(
        MassiveSearchProperties properties,
        PerimeterCsvGenerator perimeterGenerator,
        PerimeterFileRepository perimeterFileRepository,
        AnalysisWindowResolver analysisWindowResolver,
        MassiveSearchStorageService storage,
        ResultZipService resultZipService,
        SearchExecutionStepRepository stepRepository,
        MassiveSearchArtifactNaming naming,
        List<SearchReportGenerator> reportGenerators
    ) {
        this.properties = properties;
        this.perimeterGenerator = perimeterGenerator;
        this.perimeterFileRepository = perimeterFileRepository;
        this.analysisWindowResolver = analysisWindowResolver;
        this.storage = storage;
        this.resultZipService = resultZipService;
        this.stepRepository = stepRepository;
        this.naming = naming;
        this.reportGenerators = indexByType(reportGenerators);
    }

    private Map<ReportType, SearchReportGenerator> indexByType(List<SearchReportGenerator> generators) {
        Map<ReportType, SearchReportGenerator> map = new EnumMap<>(ReportType.class);
        for (SearchReportGenerator generator : generators) {
            SearchReportGenerator previous = map.put(generator.type(), generator);
            if (previous != null) {
                log.warn("Multiple report generators for type={}, using {}", generator.type(),
                    generator.getClass().getSimpleName());
            }
        }
        return map;
    }

    /** Runs the full pipeline for the given context and returns the aggregated result. */
    public EngineResult execute(MassiveSearchExecutionContext context) {
        log.info("phase=SEARCH_EXECUTION_START instanceId={} executionId={} inputType={} rerun={}",
            context.getInstanceId(), context.getExecutionId(), context.getInputType(), context.isRerun());

        // La diagnostica del perimetro e' chiusa qui, prima che i report partano: le righe di step sono
        // scritte in autocommit, quindi resta disponibile anche se un report successivo va in timeout.
        recordStep(context, StepPhase.PERIMETER, null, () -> {
            resolveInput(context);
            return context.getTotalInputRows();
        }, context::getPerimeterMetrics);
        log.info("phase=PERIMETER_READY instanceId={} executionId={} inputTemplate={} inputRows={} inputChars={}",
            context.getInstanceId(), context.getExecutionId(), context.getInputTemplate(),
            context.getTotalInputRows(),
            context.getInputCsvContent() == null ? 0 : context.getInputCsvContent().length());

        recordStep(context, StepPhase.ANALYSIS_WINDOW, null, () -> {
            context.setAnalysisWindow(analysisWindowResolver.resolve(context.getInstanceId()));
            return 0L;
        }, () -> {
            AnalysisWindow resolved = context.getAnalysisWindow();
            // La finestra e' il filtro che, se mal configurato, azzera i report in silenzio: va
            // registrata insieme al lookback di default, che e' cio' che la determina quando l'utente
            // non indica un periodo (tipico delle istanze CSV).
            return StepMetrics.create()
                .with("bounded", resolved.hasBounds())
                .with("from", resolved.fromInclusive())
                .with("to", resolved.toExclusive())
                .with("defaultLookbackMonths", properties.getExecution().getDefaultLookbackMonths())
                .with("childMarginDays", properties.getExecution().getChildDateMarginDays());
        });
        AnalysisWindow window = context.getAnalysisWindow();
        log.info("phase=ANALYSIS_WINDOW instanceId={} executionId={} bounded={} from={} to={}",
            context.getInstanceId(), context.getExecutionId(), window.hasBounds(),
            window.fromInclusive(), window.toExclusive());

        Set<ReportType> requested = context.getRequestedReports();
        if (requested == null || requested.isEmpty()) {
            requested = EnumSet.allOf(ReportType.class);
        }
        log.info("phase=REPORTS_SELECTED instanceId={} executionId={} reports={}",
            context.getInstanceId(), context.getExecutionId(), requested);

        // Capture a single timestamp so the report CSVs and the result ZIP share the same name token.
        context.setArtifactTimestamp(naming.executionTimestamp());

        // Generate only the selected reports (fixed POSITION -> TOKEN -> TRANSFER order).
        // Row counts of non-selected reports stay 0 on the context.
        List<ReportOutput> reports = new ArrayList<>();
        if (requested.contains(ReportType.POSITION)) {
            ReportOutput position = runReportStep(ReportType.POSITION, "REPORT_POSITION_START", context, window);
            context.setPositionRows(position.rows());
            reports.add(position);
        }
        if (requested.contains(ReportType.TOKEN)) {
            ReportOutput token = runReportStep(ReportType.TOKEN, "REPORT_TOKEN_START", context, window);
            context.setTokenRows(token.rows());
            reports.add(token);
        }
        if (requested.contains(ReportType.TRANSFER)) {
            ReportOutput transfer = runReportStep(ReportType.TRANSFER, "REPORT_TRANSFER_START", context, window);
            context.setTransferRows(transfer.rows());
            reports.add(transfer);
        }

        ResultZipService.ZipResult zip = zipStep(context, reports);
        log.info("phase=ZIP_CREATED instanceId={} executionId={} zipFileName={} zipPath={} sizeBytes={} reportCount={}",
            context.getInstanceId(), context.getExecutionId(), zip.zipFileName(), zip.zipPath(),
            zip.sizeBytes(), reports.size());

        cleanupIntermediateReports(context, reports);

        return new EngineResult(
            zip.zipPath(), zip.zipFileName(), zip.sizeBytes(),
            context.getTotalInputRows(), context.getPositionRows(), context.getTokenRows(), context.getTransferRows());
    }

    /** Wraps a report generation in a {@code search_execution_step} lifecycle row. */
    private ReportOutput runReportStep(ReportType type, String phase, MassiveSearchExecutionContext context,
                                       AnalysisWindow window) {
        UUID stepId = stepRepository.begin(
            context.getExecutionId(), context.getInstanceId(), StepPhase.fromReportType(type), 1, window);
        try {
            ReportOutput output = runReport(type, phase, context);
            stepRepository.complete(stepId, output.rows(), context.reportMetrics(type));
            return output;
        } catch (RuntimeException e) {
            // Anche in errore si persiste la diagnostica parziale: su un timeout e' l'unica traccia di
            // quanti batch erano passati e con che throughput, e il report successivo non partira'.
            stepRepository.fail(stepId, e.getClass().getSimpleName(), e.getMessage(), context.reportMetrics(type));
            throw e;
        }
    }

    /** Wraps the ZIP assembly in a {@code search_execution_step} lifecycle row. */
    private ResultZipService.ZipResult zipStep(MassiveSearchExecutionContext context, List<ReportOutput> reports) {
        UUID stepId = stepRepository.begin(context.getExecutionId(), context.getInstanceId(), StepPhase.ZIP, 1, null);
        try {
            ResultZipService.ZipResult zip = resultZipService.zipAndStore(context, reports);
            long totalRows = reports.stream().mapToLong(ReportOutput::rows).sum();
            stepRepository.complete(stepId, totalRows, StepMetrics.create()
                .with("sizeBytes", zip.sizeBytes())
                .with("reportCount", reports.size())
                .with("totalRows", totalRows));
            return zip;
        } catch (RuntimeException e) {
            stepRepository.fail(stepId, e.getClass().getSimpleName(), e.getMessage());
            throw e;
        }
    }

    /**
     * Wraps a pipeline phase in a {@code search_execution_step} lifecycle row.
     *
     * <p>Le metriche sono prodotte da un {@link Supplier} valutato <em>dopo</em> l'azione, cosi' il
     * chiamante puo' riferirsi a stato che l'azione stessa ha appena popolato nel contesto.</p>
     */
    private long recordStep(MassiveSearchExecutionContext context, StepPhase phase, AnalysisWindow window,
                            StepAction action, Supplier<StepMetrics> metrics) {
        UUID stepId = stepRepository.begin(context.getExecutionId(), context.getInstanceId(), phase, 1, window);
        try {
            long rows = action.run();
            stepRepository.complete(stepId, rows, safeMetrics(phase, metrics));
            return rows;
        } catch (RuntimeException e) {
            stepRepository.fail(stepId, e.getClass().getSimpleName(), e.getMessage());
            throw e;
        }
    }

    /**
     * La diagnostica e' uno strumento di supporto e non deve mai cambiare l'esito di un'esecuzione:
     * se la raccolta solleva, la fase viene chiusa senza metriche invece di far fallire la ricerca.
     */
    private StepMetrics safeMetrics(StepPhase phase, Supplier<StepMetrics> metrics) {
        try {
            return metrics.get();
        } catch (RuntimeException e) {
            log.warn("phase=STEP_METRICS_NOT_COLLECTED step={} reason={}", phase, e.getMessage());
            return null;
        }
    }

    @FunctionalInterface
    private interface StepAction {
        long run();
    }

    private void resolveInput(MassiveSearchExecutionContext context) {
        if (INPUT_TYPE_FILTER.equalsIgnoreCase(context.getInputType())) {
            PerimeterGenerationResult result = perimeterGenerator.generate(context.getInstanceId(), context.getExecutionId());
            applyPerimeter(context, result.file());
            // Null-safe: il perimetro e' la fase che produce il dato, la diagnostica e' un sottoprodotto
            // e non deve poter far fallire l'esecuzione se manca.
            StepMetrics metrics = result.metrics() == null ? StepMetrics.create() : result.metrics();
            context.setPerimeterMetrics(metrics.with("source", INPUT_TYPE_FILTER));
        } else if (INPUT_TYPE_CSV.equalsIgnoreCase(context.getInputType())) {
            PerimeterFileMetadata uploaded = perimeterFileRepository.findLatestUploaded(context.getInstanceId())
                .orElseThrow(() -> new MassiveSearchExecutionException(
                    "No uploaded CSV perimeter found for instance " + context.getInstanceId()));
            applyPerimeter(context, uploaded);
            // Perimetro caricato dall'utente: non c'e' una query da misurare, ma il template
            // riconosciuto e il numero di righe spiegano gia' molto di un report inatteso.
            context.setPerimeterMetrics(StepMetrics.create()
                .with("source", INPUT_TYPE_CSV)
                .with("template", uploaded.template())
                .with("resolvedTemplate", context.getInputTemplate())
                .with("rows", uploaded.rowsCount()));
        } else {
            throw new MassiveSearchExecutionException("Unsupported input type: " + context.getInputType());
        }
    }

    private void applyPerimeter(MassiveSearchExecutionContext context, PerimeterFileMetadata file) {
        context.setInputCsvPath(file.filePath());
        context.setInputCsvContent(file.content());
        context.setPerimeterFileId(file.id());
        context.setInputTemplate(parseTemplate(file.template()));
        context.setTotalInputRows(file.rowsCount());
    }

    private CsvTemplate parseTemplate(String template) {
        if (template == null) {
            return CsvTemplate.UNKNOWN;
        }
        try {
            return CsvTemplate.valueOf(template);
        } catch (IllegalArgumentException e) {
            return CsvTemplate.UNKNOWN;
        }
    }

    private ReportOutput runReport(ReportType type, String phase, MassiveSearchExecutionContext context) {
        String fileName = fileNameFor(type, context);
        String relativePath = properties.getStorage().executionObjectPath(context.getExecutionId(), fileName);
        Charset charset = properties.getCsv().getCharset();

        log.info("phase={} instanceId={} executionId={} file={}", phase,
            context.getInstanceId(), context.getExecutionId(), fileName);

        MassiveSearchStorageService.StoredObject stored = storage.saveExecutionCsv(relativePath, charset, writer -> {
            SearchReportGenerator generator = reportGenerators.get(type);
            if (generator == null) {
                // TODO: replaced by the dedicated report generator (packages massivesearch.report.*).
                log.warn("phase={} instanceId={} executionId={} no generator for type={}, writing empty report",
                    phase, context.getInstanceId(), context.getExecutionId(), type);
                return 0L;
            }
            return generator.writeReport(context, writer);
        });
        return new ReportOutput(type, stored.path(), fileName, stored.rows());
    }

    private void cleanupIntermediateReports(MassiveSearchExecutionContext context, List<ReportOutput> reports) {
        for (ReportOutput report : reports) {
            try {
                storage.delete(report.storagePath());
            } catch (RuntimeException e) {
                // Best-effort: the ZIP already holds the data, a leftover CSV must not fail the run.
                log.warn("phase=REPORT_CLEANUP_FAILED instanceId={} executionId={} storagePath={} reason={}",
                    context.getInstanceId(), context.getExecutionId(), report.storagePath(), e.getMessage());
            }
        }
    }

    private String fileNameFor(ReportType type, MassiveSearchExecutionContext context) {
        MassiveSearchProperties.Reports reports = properties.getReports();
        String prefix = switch (type) {
            case POSITION -> reports.getPositionPrefix();
            case TOKEN -> reports.getTokenPrefix();
            case TRANSFER -> reports.getTransferPrefix();
        };
        return naming.reportFileName(prefix, context.getExecutionId(), context.getArtifactTimestamp());
    }
}
