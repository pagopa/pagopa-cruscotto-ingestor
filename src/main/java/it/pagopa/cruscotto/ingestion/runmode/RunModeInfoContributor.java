package it.pagopa.cruscotto.ingestion.runmode;

import it.pagopa.cruscotto.ingestion.configuration.AppModeProperties;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Adds the active run mode and the scheduler running-state to {@code /management/info}, so the mode is
 * visible at a glance alongside the build/git info. No sensitive data is exposed.
 */
@Component
public class RunModeInfoContributor implements InfoContributor {

    private final AppModeProperties appModeProperties;
    private final Scheduler scheduler;

    public RunModeInfoContributor(AppModeProperties appModeProperties, Scheduler scheduler) {
        this.appModeProperties = appModeProperties;
        this.scheduler = scheduler;
    }

    @Override
    public void contribute(Info.Builder builder) {
        Map<String, Object> runMode = new LinkedHashMap<>();
        runMode.put("mode", appModeProperties.getMode());
        runMode.put("schedulerEnabled", appModeProperties.isSchedulerEnabled());
        runMode.put("ingestionActive", appModeProperties.ingestionActive());
        runMode.put("massiveSearchActive", appModeProperties.massiveSearchActive());
        runMode.put("schedulerRunning", schedulerRunning());
        builder.withDetail("runMode", runMode);
    }

    private boolean schedulerRunning() {
        try {
            return scheduler.isStarted() && !scheduler.isInStandbyMode() && !scheduler.isShutdown();
        } catch (SchedulerException e) {
            return false;
        }
    }
}