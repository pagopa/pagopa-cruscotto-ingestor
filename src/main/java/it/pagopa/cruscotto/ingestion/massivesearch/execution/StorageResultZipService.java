package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.naming.MassiveSearchArtifactNaming;
import it.pagopa.cruscotto.ingestion.massivesearch.storage.MassiveSearchStorageService;
import it.pagopa.cruscotto.ingestion.massivesearch.storage.MassiveSearchStorageService.StoredBinary;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Storage-agnostic {@link ResultZipService}: reads the three per-execution report CSVs through the
 * {@link MassiveSearchStorageService} port and streams the assembled result ZIP back through
 * the same port. Works identically for the local filesystem and the Azure Blob backends, so the
 * archive is created wherever the reports were stored without buffering it in memory.
 */
@Slf4j
@Service
public class StorageResultZipService implements ResultZipService {

    private final MassiveSearchProperties properties;
    private final MassiveSearchStorageService storage;
    private final MassiveSearchArtifactNaming naming;

    public StorageResultZipService(MassiveSearchProperties properties,
                                   MassiveSearchStorageService storage,
                                   MassiveSearchArtifactNaming naming) {
        this.properties = properties;
        this.storage = storage;
        this.naming = naming;
    }

    @Override
    public ZipResult zipAndStore(MassiveSearchExecutionContext context, List<ReportOutput> reports) {
        String zipFileName = naming.resultZipFileName(context.getExecutionId());
        String relativePath = properties.getStorage().executionObjectPath(context.getExecutionId(), zipFileName);

        StoredBinary stored = storage.saveResultZip(relativePath, out -> writeZip(out, reports));
        return new ZipResult(stored.path(), zipFileName, stored.sizeBytes());
    }

    private void writeZip(OutputStream out, List<ReportOutput> reports) throws IOException {
        ZipOutputStream zip = new ZipOutputStream(out);
        for (ReportOutput report : reports) {
            zip.putNextEntry(new ZipEntry(report.fileName()));
            copyReport(report, zip);
            zip.closeEntry();
        }
        // finish (write central directory) without closing the underlying stream, which is owned and
        // closed by the storage backend.
        zip.finish();
        zip.flush();
    }

    private void copyReport(ReportOutput report, ZipOutputStream zip) throws IOException {
        // Do not swallow read failures: a missing/unreadable report must fail the whole execution
        // so it is marked FAILED, instead of silently producing an incomplete result.zip.
        try (InputStream in = storage.openForRead(report.storagePath())) {
            in.transferTo(zip);
        }
    }
}
