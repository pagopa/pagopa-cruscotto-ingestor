package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
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
    private final Map<ReportType, SearchReportGenerator> reportGenerators;

    public MassiveSearchEngine(
        MassiveSearchProperties properties,
        PerimeterCsvGenerator perimeterGenerator,
        PerimeterFileRepository perimeterFileRepository,
        AnalysisWindowResolver analysisWindowResolver,
        MassiveSearchStorageService storage,
        ResultZipService resultZipService,
        SearchExecutionStepRepository stepRepository,
        List<SearchReportGenerator> reportGenerators
    ) {
        this.properties = properties;
        this.perimeterGenerator = perimeterGenerator;
        this.perimeterFileRepository = perimeterFileRepository;
        this.analysisWindowResolver = analysisWindowResolver;
        this.storage = storage;
        this.resultZipService = resultZipService;
        this.stepRepository = stepRepository;
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

        recordStep(context, StepPhase.PERIMETER, null, () -> {
            resolveInput(context);
            return context.getTotalInputRows();
        });
        log.info("phase=PERIMETER_READY instanceId={} executionId={} inputTemplate={} inputRows={} inputChars={}",
            context.getInstanceId(), context.getExecutionId(), context.getInputTemplate(),
            context.getTotalInputRows(),
            context.getInputCsvContent() == null ? 0 : context.getInputCsvContent().length());

        recordStep(context, StepPhase.ANALYSIS_WINDOW, null, () -> {
            context.setAnalysisWindow(analysisWindowResolver.resolve(context.getInstanceId()));
            return 0L;
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
            context.setAttemptRows(token.rows());
            reports.add(token);
        }
        if (requested.contains(ReportType.TRANSFER)) {
            ReportOutput transfer = runReportStep(ReportType.TRANSFER, "REPORT_TRANSFER_START", context, window);
            context.setTransferRows(transfer.rows());
            reports.add(transfer);
        }

        ResultZipService.ZipResult zip = zipStep(context, reports);
        log.info("phase=ZIP_CREATED instanceId={} executionId={} zipPath={} sizeBytes={} reportCount={}",
            context.getInstanceId(), context.getExecutionId(), zip.zipPath(), zip.sizeBytes(), reports.size());

        cleanupIntermediateReports(context, reports);

        return new EngineResult(
            zip.zipPath(), zip.zipFileName(), zip.sizeBytes(),
            context.getTotalInputRows(), context.getPositionRows(), context.getAttemptRows(), context.getTransferRows());
    }

    /** Wraps a report generation in a {@code search_execution_step} lifecycle row. */
    private ReportOutput runReportStep(ReportType type, String phase, MassiveSearchExecutionContext context,
                                       AnalysisWindow window) {
        UUID stepId = stepRepository.begin(
            context.getExecutionId(), context.getInstanceId(), StepPhase.fromReportType(type), 1, window);
        try {
            ReportOutput output = runReport(type, phase, context);
            stepRepository.complete(stepId, output.rows());
            return output;
        } catch (RuntimeException e) {
            stepRepository.fail(stepId, e.getClass().getSimpleName(), e.getMessage());
            throw e;
        }
    }

    /** Wraps the ZIP assembly in a {@code search_execution_step} lifecycle row. */
    private ResultZipService.ZipResult zipStep(MassiveSearchExecutionContext context, List<ReportOutput> reports) {
        UUID stepId = stepRepository.begin(context.getExecutionId(), context.getInstanceId(), StepPhase.ZIP, 1, null);
        try {
            ResultZipService.ZipResult zip = resultZipService.zipAndStore(context, reports);
            long totalRows = reports.stream().mapToLong(ReportOutput::rows).sum();
            stepRepository.complete(stepId, totalRows);
            return zip;
        } catch (RuntimeException e) {
            stepRepository.fail(stepId, e.getClass().getSimpleName(), e.getMessage());
            throw e;
        }
    }

    /** Wraps a pipeline phase in a {@code search_execution_step} lifecycle row. */
    private long recordStep(MassiveSearchExecutionContext context, StepPhase phase, AnalysisWindow window,
                            StepAction action) {
        UUID stepId = stepRepository.begin(context.getExecutionId(), context.getInstanceId(), phase, 1, window);
        try {
            long rows = action.run();
            stepRepository.complete(stepId, rows);
            return rows;
        } catch (RuntimeException e) {
            stepRepository.fail(stepId, e.getClass().getSimpleName(), e.getMessage());
            throw e;
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
        } else if (INPUT_TYPE_CSV.equalsIgnoreCase(context.getInputType())) {
            PerimeterFileMetadata uploaded = perimeterFileRepository.findLatestUploaded(context.getInstanceId())
                .orElseThrow(() -> new MassiveSearchExecutionException(
                    "No uploaded CSV perimeter found for instance " + context.getInstanceId()));
            applyPerimeter(context, uploaded);
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
        String fileName = fileNameFor(type);
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

    private String fileNameFor(ReportType type) {
        MassiveSearchProperties.Reports reports = properties.getReports();
        return switch (type) {
            case POSITION -> reports.getPositionFileName();
            case TOKEN -> reports.getAttemptFileName();
            case TRANSFER -> reports.getTransferFileName();
        };
    }
}
