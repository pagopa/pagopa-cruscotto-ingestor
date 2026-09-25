package it.pagopa.cruscotto.ingestion.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Run-mode configuration, bound from the {@code app:} section.
 *
 * <p>Two orthogonal knobs:</p>
 * <ul>
 *   <li>{@code app.mode} ({@link AppRunMode}) selects <em>what</em> runs: {@code INGESTOR},
 *       {@code MASSIVE_SEARCH} or {@code BOTH};</li>
 *   <li>{@code app.scheduler-enabled} is the master ON/OFF (stop/restart) that gates <em>whether</em>
 *       the shared Quartz scheduler auto-starts at all.</li>
 * </ul>
 *
 * <p>They combine so the scheduler starts only when it is enabled AND the mode requires at least one
 * subsystem. Defaults preserve the historical behaviour ({@code mode=BOTH}, scheduler enabled). In
 * {@code application.yml} the flag reads {@code SCHEDULER_ENABLED} and falls back to the legacy
 * {@code INGESTION_QUARTZ_ENABLED} for backward compatibility.</p>
 */
@Configuration
@ConfigurationProperties(prefix = "app")
@Getter
@Setter
public class AppModeProperties {

    /** Which subsystems run: {@code INGESTOR}, {@code MASSIVE_SEARCH} or {@code BOTH}. */
    private AppRunMode mode = AppRunMode.BOTH;

    /** Master switch: when {@code false} the Quartz scheduler does not start (everything paused). */
    private boolean schedulerEnabled = true;

    /** @return {@code true} when the ADX ingestion jobs must be scheduled. */
    public boolean ingestionActive() {
        return schedulerEnabled && mode.includesIngestor();
    }

    /** @return {@code true} when the Massive Search scanner must be scheduled. */
    public boolean massiveSearchActive() {
        return schedulerEnabled && mode.includesMassiveSearch();
    }

    /** @return {@code true} when the shared Quartz scheduler must auto-start (at least one subsystem active). */
    public boolean schedulerShouldStart() {
        return ingestionActive() || massiveSearchActive();
    }
}