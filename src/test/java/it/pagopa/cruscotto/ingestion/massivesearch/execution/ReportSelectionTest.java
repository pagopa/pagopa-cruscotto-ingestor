package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReportSelectionTest {

    private static final Set<ReportType> ALL = EnumSet.allOf(ReportType.class);

    @Test
    void nullOrBlankMeansAllReports() {
        assertEquals(ALL, ReportSelection.parse(null));
        assertEquals(ALL, ReportSelection.parse(""));
        assertEquals(ALL, ReportSelection.parse("   "));
    }

    @Test
    void fullSelectionIsAllThree() {
        assertEquals(ALL, ReportSelection.parse("POSITION,TOKEN,TRANSFER"));
    }

    @Test
    void singleAndSubsetSelections() {
        assertEquals(EnumSet.of(ReportType.POSITION), ReportSelection.parse("POSITION"));
        assertEquals(EnumSet.of(ReportType.POSITION, ReportType.TRANSFER),
                ReportSelection.parse("POSITION,TRANSFER"));
        assertEquals(EnumSet.of(ReportType.TOKEN), ReportSelection.parse("TOKEN"));
    }

    @Test
    void toleratesWhitespaceAndCase() {
        assertEquals(EnumSet.of(ReportType.POSITION, ReportType.TRANSFER),
                ReportSelection.parse("  position , TRANSFER "));
    }

    @Test
    void unknownNamesAreIgnoredAndPureUnknownFallsBackToAll() {
        // Unknown token ignored, known kept.
        assertEquals(EnumSet.of(ReportType.POSITION), ReportSelection.parse("POSITION,ATTEMPT"));
        // Only unknowns -> safe fallback to all.
        assertEquals(ALL, ReportSelection.parse("BOGUS,ATTEMPT"));
    }
}
