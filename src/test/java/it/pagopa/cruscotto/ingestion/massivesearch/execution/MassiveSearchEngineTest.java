package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterCsvGenerator;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterFileMetadata;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterFileRepository;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterGenerationResult;
import it.pagopa.cruscotto.ingestion.massivesearch.storage.MassiveSearchStorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that the engine generates, zips and cleans up ONLY the report types selected on the
 * execution context (selective report production), while always resolving the perimeter and window.
 */
class MassiveSearchEngineTest {

    private MassiveSearchProperties properties;
    private PerimeterCsvGenerator perimeterGenerator;
    private PerimeterFileRepository perimeterFileRepository;
    private AnalysisWindowResolver analysisWindowResolver;
    private MassiveSearchStorageService storage;
    private ResultZipService resultZipService;
    private SearchExecutionStepRepository stepRepository;

    private MassiveSearchEngine engine;

    private final UUID instanceId = UUID.randomUUID();
    private final UUID executionId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        properties = mock(MassiveSearchProperties.class, RETURNS_DEEP_STUBS);
        perimeterGenerator = mock(PerimeterCsvGenerator.class);
        perimeterFileRepository = mock(PerimeterFileRepository.class);
        analysisWindowResolver = mock(AnalysisWindowResolver.class);
        storage = mock(MassiveSearchStorageService.class);
        resultZipService = mock(ResultZipService.class);
        stepRepository = mock(SearchExecutionStepRepository.class);

        SearchReportGenerator positionGen = mock(SearchReportGenerator.class);
        when(positionGen.type()).thenReturn(ReportType.POSITION);
        SearchReportGenerator tokenGen = mock(SearchReportGenerator.class);
        when(tokenGen.type()).thenReturn(ReportType.TOKEN);
        SearchReportGenerator transferGen = mock(SearchReportGenerator.class);
        when(transferGen.type()).thenReturn(ReportType.TRANSFER);

        engine = new MassiveSearchEngine(properties, perimeterGenerator, perimeterFileRepository,
            analysisWindowResolver, storage, resultZipService, stepRepository,
            List.of(positionGen, tokenGen, transferGen));

        lenient().when(properties.getStorage().executionObjectPath(any(), anyString())).thenReturn("exec/path.csv");
        lenient().when(properties.getCsv().getCharset()).thenReturn(StandardCharsets.UTF_8);
        lenient().when(properties.getReports().getPositionFileName()).thenReturn("posizioni.csv");
        lenient().when(properties.getReports().getAttemptFileName()).thenReturn("tentativi.csv");
        lenient().when(properties.getReports().getTransferFileName()).thenReturn("versamenti.csv");

        lenient().when(stepRepository.begin(any(), any(), any(), anyInt(), any())).thenReturn(UUID.randomUUID());
        lenient().when(analysisWindowResolver.resolve(any())).thenReturn(AnalysisWindow.none());

        PerimeterFileMetadata file = mock(PerimeterFileMetadata.class);
        lenient().when(file.filePath()).thenReturn("input.csv");
        lenient().when(file.content()).thenReturn("NAV;EC\n");
        lenient().when(file.id()).thenReturn(UUID.randomUUID());
        lenient().when(file.template()).thenReturn("UNKNOWN");
        lenient().when(file.rowsCount()).thenReturn(10L);
        lenient().when(perimeterGenerator.generate(any(), any())).thenReturn(new PerimeterGenerationResult(file, false));

        lenient().when(storage.saveExecutionCsv(anyString(), any(Charset.class), any()))
            .thenReturn(new MassiveSearchStorageService.StoredObject("stored/report.csv", 5L));
        lenient().when(resultZipService.zipAndStore(any(), any()))
            .thenReturn(new ResultZipService.ZipResult("stored/result.zip", "result.zip", 123L));
    }

    private MassiveSearchExecutionContext context(EnumSet<ReportType> reports) {
        MassiveSearchExecutionContext ctx =
            new MassiveSearchExecutionContext(instanceId, executionId, "FILTER", false);
        ctx.setRequestedReports(reports);
        return ctx;
    }

    @Test
    @SuppressWarnings("unchecked")
    void generatesOnlyTheSelectedReport() {
        MassiveSearchExecutionContext ctx = context(EnumSet.of(ReportType.POSITION));

        engine.execute(ctx);

        // Exactly one report CSV produced (only POSITION), and the ZIP receives only that report.
        verify(storage, times(1)).saveExecutionCsv(anyString(), any(Charset.class), any());
        org.mockito.ArgumentCaptor<List<ReportOutput>> zipCaptor =
            org.mockito.ArgumentCaptor.forClass(List.class);
        verify(resultZipService).zipAndStore(eq(ctx), zipCaptor.capture());
        assertEquals(1, zipCaptor.getValue().size());
        assertEquals(ReportType.POSITION, zipCaptor.getValue().get(0).type());
        // Non-selected reports report zero rows.
        assertEquals(0L, ctx.getAttemptRows());
        assertEquals(0L, ctx.getTransferRows());
    }

    @Test
    @SuppressWarnings("unchecked")
    void generatesAllThreeWhenAllSelected() {
        MassiveSearchExecutionContext ctx = context(EnumSet.allOf(ReportType.class));

        engine.execute(ctx);

        verify(storage, times(3)).saveExecutionCsv(anyString(), any(Charset.class), any());
        org.mockito.ArgumentCaptor<List<ReportOutput>> zipCaptor =
            org.mockito.ArgumentCaptor.forClass(List.class);
        verify(resultZipService).zipAndStore(eq(ctx), zipCaptor.capture());
        assertEquals(3, zipCaptor.getValue().size());
        assertTrue(zipCaptor.getValue().stream().anyMatch(r -> r.type() == ReportType.TOKEN));
    }

    @Test
    @SuppressWarnings("unchecked")
    void skipsPositionWhenNotSelected() {
        MassiveSearchExecutionContext ctx = context(EnumSet.of(ReportType.TOKEN, ReportType.TRANSFER));

        engine.execute(ctx);

        verify(storage, times(2)).saveExecutionCsv(anyString(), any(Charset.class), any());
        org.mockito.ArgumentCaptor<List<ReportOutput>> zipCaptor =
            org.mockito.ArgumentCaptor.forClass(List.class);
        verify(resultZipService).zipAndStore(eq(ctx), zipCaptor.capture());
        assertEquals(2, zipCaptor.getValue().size());
        assertTrue(zipCaptor.getValue().stream().noneMatch(r -> r.type() == ReportType.POSITION));
        assertEquals(0L, ctx.getPositionRows());
        // step for POSITION phase never opened
        verify(stepRepository, never()).begin(any(), any(), eq(StepPhase.POSITION), anyInt(), any());
    }
}
