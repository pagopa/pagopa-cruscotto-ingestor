package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies {@link CsvInputReader#newReader(String)}: the perimeter CSV is now read from the in-memory
 * content stored in the DB instead of a blob/filesystem stream, and must yield exactly the same lines.
 */
class CsvInputReaderTest {

    private final CsvInputReader reader = newInputReader();

    private static CsvInputReader newInputReader() {
        MassiveSearchProperties properties = mock(MassiveSearchProperties.class, RETURNS_DEEP_STUBS);
        lenient().when(properties.getCsv().getSeparator()).thenReturn(";");
        lenient().when(properties.getCsv().getCharset()).thenReturn(StandardCharsets.UTF_8);
        return new CsvInputReader(properties);
    }

    @Test
    void readsAllLinesFromInMemoryContent() throws IOException {
        try (BufferedReader r = reader.newReader("NAV;EC\nabc;123\ndef;456\n")) {
            assertEquals("NAV;EC", r.readLine());
            assertEquals("abc;123", r.readLine());
            assertEquals("def;456", r.readLine());
            assertNull(r.readLine());
        }
    }

    @Test
    void nullContentIsTreatedAsEmptyInput() throws IOException {
        try (BufferedReader r = reader.newReader((String) null)) {
            assertNull(r.readLine());
        }
    }
}
