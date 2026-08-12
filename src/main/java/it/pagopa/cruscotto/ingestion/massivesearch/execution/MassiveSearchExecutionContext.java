package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import lombok.Getter;
import lombok.Setter;

import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

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
    private long attemptRows;
    private long transferRows;

    /**
     * Report types to produce for this execution (GUI checkbox selection). Defaults to all three so
     * an execution created without an explicit selection keeps the historical always-three behaviour.
     */
    private Set<ReportType> requestedReports = EnumSet.allOf(ReportType.class);

    /** Optional temporal window limiting the analysis; never {@code null}. */
    private AnalysisWindow analysisWindow = AnalysisWindow.none();

    public MassiveSearchExecutionContext(UUID instanceId, UUID executionId, String inputType, boolean rerun) {
        this.instanceId = instanceId;
        this.executionId = executionId;
        this.inputType = inputType;
        this.rerun = rerun;
    }
}
