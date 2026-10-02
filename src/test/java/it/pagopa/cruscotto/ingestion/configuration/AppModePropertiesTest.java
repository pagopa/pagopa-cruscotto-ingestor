package it.pagopa.cruscotto.ingestion.configuration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the run-mode decision logic (what runs, and whether the scheduler starts) across every
 * combination of {@link AppRunMode} and the master switch.
 */
class AppModePropertiesTest {

    private AppModeProperties props(AppRunMode mode, boolean enabled) {
        AppModeProperties p = new AppModeProperties();
        p.setMode(mode);
        p.setSchedulerEnabled(enabled);
        return p;
    }

    @Test
    void defaultsAreBothAndEnabled() {
        AppModeProperties p = new AppModeProperties();
        assertEquals(AppRunMode.BOTH, p.getMode());
        assertTrue(p.isSchedulerEnabled());
        assertTrue(p.ingestionActive());
        assertTrue(p.massiveSearchActive());
        assertTrue(p.schedulerShouldStart());
    }

    @Test
    void ingestorModeRunsOnlyIngestion() {
        AppModeProperties p = props(AppRunMode.INGESTOR, true);
        assertTrue(p.ingestionActive());
        assertFalse(p.massiveSearchActive());
        assertTrue(p.schedulerShouldStart());
    }

    @Test
    void massiveSearchModeRunsOnlyScanner() {
        AppModeProperties p = props(AppRunMode.MASSIVE_SEARCH, true);
        assertFalse(p.ingestionActive());
        assertTrue(p.massiveSearchActive());
        assertTrue(p.schedulerShouldStart());
    }

    @Test
    void bothModeRunsEverything() {
        AppModeProperties p = props(AppRunMode.BOTH, true);
        assertTrue(p.ingestionActive());
        assertTrue(p.massiveSearchActive());
        assertTrue(p.schedulerShouldStart());
    }

    @Test
    void masterSwitchOffPausesEverythingInEveryMode() {
        for (AppRunMode mode : AppRunMode.values()) {
            AppModeProperties p = props(mode, false);
            assertFalse(p.ingestionActive(), mode.name());
            assertFalse(p.massiveSearchActive(), mode.name());
            assertFalse(p.schedulerShouldStart(), mode.name());
        }
    }

    @Test
    void enumMembership() {
        assertTrue(AppRunMode.INGESTOR.includesIngestor());
        assertFalse(AppRunMode.INGESTOR.includesMassiveSearch());
        assertFalse(AppRunMode.MASSIVE_SEARCH.includesIngestor());
        assertTrue(AppRunMode.MASSIVE_SEARCH.includesMassiveSearch());
        assertTrue(AppRunMode.BOTH.includesIngestor());
        assertTrue(AppRunMode.BOTH.includesMassiveSearch());
    }
}