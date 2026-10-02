package it.pagopa.cruscotto.ingestion.massivesearch.csv;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the perimeter CSV template is deduced from the header row per the client mapping:
 * {@code NAV;EC -> NAV_PA}, {@code IUV;EC -> IUV_PA}, {@code NAV -> NAV}, {@code IUV -> IUV},
 * {@code Token -> TOKEN}. "EC" is the idDominio/Ente Creditore column header.
 */
class CsvTemplateDetectorTest {

    private final CsvTemplateDetector detector = new CsvTemplateDetector();

    @Test
    void navPlusEcIsNavPa() {
        CsvTemplateDetector.TemplateDetection d = detector.detect(List.of("NAV", "EC"));
        assertEquals(CsvTemplate.NAV_PA, d.template());
        assertEquals(0, d.navIndex());
        assertEquals(1, d.paIndex());
    }

    @Test
    void iuvPlusEcIsIuvPa() {
        CsvTemplateDetector.TemplateDetection d = detector.detect(List.of("IUV", "EC"));
        assertEquals(CsvTemplate.IUV_PA, d.template());
        assertEquals(0, d.iuvIndex());
        assertEquals(1, d.paIndex());
    }

    @Test
    void singleColumnTemplates() {
        assertEquals(CsvTemplate.NAV, detector.detect(List.of("NAV")).template());
        assertEquals(CsvTemplate.IUV, detector.detect(List.of("IUV")).template());
        assertEquals(CsvTemplate.TOKEN, detector.detect(List.of("Token")).template());
    }

    @Test
    void headerMatchingIsCaseInsensitive() {
        assertEquals(CsvTemplate.NAV_PA, detector.detect(List.of("nav", "ec")).template());
    }

    @Test
    void unknownHeaderIsUnknownTemplate() {
        assertEquals(CsvTemplate.UNKNOWN, detector.detect(List.of("FOO")).template());
        assertEquals(CsvTemplate.UNKNOWN, detector.detect(List.of()).template());
    }
}
