package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import java.util.List;

/**
 * Assembles the three per-execution report CSVs into the final {@code result.zip} and stores it.
 *
 * <p>Implemented by {@code StorageResultZipService} on top of the {@code MassiveSearchStorageService}
 * port, so the archive is produced on whichever backend (local filesystem or Azure Blob) is active.</p>
 */
public interface ResultZipService {

    /**
     * Zips the given report files and stores the resulting archive for the execution.
     *
     * @param context the current execution context
     * @param reports the generated report files to include
     * @return the stored ZIP descriptor
     */
    ZipResult zipAndStore(MassiveSearchExecutionContext context, List<ReportOutput> reports);

    /** Descriptor of the stored result ZIP. */
    record ZipResult(String zipPath, String zipFileName, long sizeBytes) {}
}
