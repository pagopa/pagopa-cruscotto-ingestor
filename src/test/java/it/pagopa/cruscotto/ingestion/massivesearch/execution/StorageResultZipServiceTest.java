package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.naming.MassiveSearchArtifactNaming;
import it.pagopa.cruscotto.ingestion.massivesearch.storage.MassiveSearchStorageService;
import it.pagopa.cruscotto.ingestion.massivesearch.storage.MassiveSearchStorageService.BinaryWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.storage.MassiveSearchStorageService.StoredBinary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Locks the naming contract of the result ZIP: the archive file name follows the convention with the
 * execution short id and the shared execution timestamp, and the entries inside it are exactly the
 * report CSV file names (so archive and contents read as a single, self-describing set).
 */
class StorageResultZipServiceTest {

    private MassiveSearchProperties properties;
    private MassiveSearchStorageService storage;
    private MassiveSearchArtifactNaming naming;
    private StorageResultZipService service;

    private final UUID executionId = UUID.fromString("a1b2c3d4-0000-0000-0000-000000000000");
    private final LocalDateTime timestamp = LocalDateTime.of(2026, 8, 4, 15, 35, 0);

    @BeforeEach
    void setUp() {
        properties = new MassiveSearchProperties();
        storage = mock(MassiveSearchStorageService.class);
        naming = new MassiveSearchArtifactNaming(properties);
        service = new StorageResultZipService(properties, storage, naming);
    }

    @Test
    void zipNameAndEntriesFollowTheSharedNamingConvention() throws Exception {
        MassiveSearchExecutionContext ctx =
            new MassiveSearchExecutionContext(UUID.randomUUID(), executionId, "FILTER", false);
        ctx.setArtifactTimestamp(timestamp);

        // Two reports whose file names are built exactly as the engine builds them.
        String posName = naming.reportFileName(properties.getReports().getPositionPrefix(), executionId, timestamp);
        String attName = naming.reportFileName(properties.getReports().getTokenPrefix(), executionId, timestamp);
        Map<String, byte[]> reportBytes = new LinkedHashMap<>();
        reportBytes.put("stored/pos.csv", "NAV,PA\n1,2\n".getBytes(StandardCharsets.UTF_8));
        reportBytes.put("stored/att.csv", "NAV,PA\n3,4\n".getBytes(StandardCharsets.UTF_8));
        List<ReportOutput> reports = List.of(
            new ReportOutput(ReportType.POSITION, "stored/pos.csv", posName, 1L),
            new ReportOutput(ReportType.TOKEN, "stored/att.csv", attName, 1L));

        // Each read returns a fresh stream over the report bytes.
        when(storage.openForRead(anyString()))
            .thenAnswer(inv -> new ByteArrayInputStream(reportBytes.get(inv.getArgument(0, String.class))));

        // Run the real ZIP assembly and capture the produced archive bytes.
        ByteArrayOutputStream zipBytes = new ByteArrayOutputStream();
        when(storage.saveResultZip(anyString(), any(BinaryWriter.class))).thenAnswer(inv -> {
            inv.getArgument(1, BinaryWriter.class).writeTo(zipBytes);
            return new StoredBinary(inv.getArgument(0, String.class), zipBytes.size());
        });

        ResultZipService.ZipResult result = service.zipAndStore(ctx, reports);

        // ZIP file name: ricerca-massiva__<8hex-execId>__<ts>.zip
        assertEquals("ricerca-massiva__a1b2c3d4__20260804-153500.zip", result.zipFileName());

        // Stored under the per-execution folder, path ends with the zip file name.
        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        org.mockito.Mockito.verify(storage).saveResultZip(pathCaptor.capture(), any(BinaryWriter.class));
        assertTrue(pathCaptor.getValue().endsWith("/" + result.zipFileName()), pathCaptor.getValue());
        assertTrue(pathCaptor.getValue().contains(executionId.toString()), pathCaptor.getValue());

        // Entries inside the ZIP are exactly the report file names, with the report content preserved.
        Map<String, byte[]> entries = readZip(zipBytes.toByteArray());
        assertEquals(List.of(posName, attName), new ArrayList<>(entries.keySet()));
        assertArrayEquals(reportBytes.get("stored/pos.csv"), entries.get(posName));
        assertArrayEquals(reportBytes.get("stored/att.csv"), entries.get(attName));
    }

    private Map<String, byte[]> readZip(byte[] bytes) throws Exception {
        Map<String, byte[]> out = new LinkedHashMap<>();
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(bytes))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                out.put(entry.getName(), zis.readAllBytes());
                zis.closeEntry();
            }
        }
        return out;
    }
}