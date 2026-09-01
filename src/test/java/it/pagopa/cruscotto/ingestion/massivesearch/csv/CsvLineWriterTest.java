package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvLineWriterTest {

    private final CsvLineWriter writer = new CsvLineWriter(new MassiveSearchProperties());

    @Test
    void writesPlainValuesJoinedBySeparatorAndTerminatedByCrlf() throws IOException {
        StringWriter out = new StringWriter();
        writer.writeLine(out, List.of("a", "b", "c"));
        assertEquals("a,b,c\r\n", out.toString());
    }

    @Test
    void rendersNullValueAsEmptyField() throws IOException {
        StringWriter out = new StringWriter();
        writer.writeLine(out, Arrays.asList("a", null, "c"));
        assertEquals("a,,c\r\n", out.toString());
    }

    @Test
    void quotesValuesContainingSeparatorQuoteOrNewline() throws IOException {
        StringWriter out = new StringWriter();
        writer.writeLine(out, List.of("a,b", "he said \"hi\"", "line1\nline2"));
        assertEquals("\"a,b\",\"he said \"\"hi\"\"\",\"line1\nline2\"\r\n", out.toString());
    }
}
