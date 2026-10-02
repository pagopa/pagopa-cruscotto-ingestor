package it.pagopa.cruscotto.ingestion.runmode;

import it.pagopa.cruscotto.ingestion.configuration.AppModeProperties;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerMetaData;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only actuator endpoint {@code /management/runmode}: reports the active run mode, the shared
 * Quartz scheduler state and every scheduled trigger with its last/next fire time, so operators can
 * tell whether the ingestor / massive-search is actually running without inspecting the DB log table.
 *
 * <p>Exposes only operational metadata (names, booleans, counts, timestamps): no connection strings,
 * secrets or business data.</p>
 */
@Slf4j
@Component
@Endpoint(id = "runmode")
public class RunModeEndpoint {

    private final AppModeProperties appModeProperties;
    private final Scheduler scheduler;

    public RunModeEndpoint(AppModeProperties appModeProperties, Scheduler scheduler) {
        this.appModeProperties = appModeProperties;
        this.scheduler = scheduler;
    }

    @ReadOperation
    public Map<String, Object> runMode() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("mode", appModeProperties.getMode());
        out.put("schedulerEnabled", appModeProperties.isSchedulerEnabled());
        out.put("ingestionActive", appModeProperties.ingestionActive());
        out.put("massiveSearchActive", appModeProperties.massiveSearchActive());
        out.put("scheduler", schedulerInfo());
        out.put("triggers", triggers());
        return out;
    }

    private Map<String, Object> schedulerInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        try {
            info.put("running", scheduler.isStarted() && !scheduler.isInStandbyMode() && !scheduler.isShutdown());
            info.put("standby", scheduler.isInStandbyMode());
            info.put("shutdown", scheduler.isShutdown());
            info.put("currentlyExecutingJobs", scheduler.getCurrentlyExecutingJobs().size());
            SchedulerMetaData md = scheduler.getMetaData();
            info.put("schedulerName", md.getSchedulerName());
            info.put("instanceId", md.getSchedulerInstanceId());
            info.put("clustered", md.isJobStoreClustered());
            info.put("threadPoolSize", md.getThreadPoolSize());
            info.put("runningSince", md.getRunningSince() == null ? null : md.getRunningSince().toInstant().toString());
            info.put("jobsExecuted", md.getNumberOfJobsExecuted());
        } catch (SchedulerException e) {
            info.put("error", e.getMessage());
        }
        return info;
    }

    private List<Map<String, Object>> triggers() {
        List<Map<String, Object>> list = new ArrayList<>();
        try {
            for (String group : scheduler.getTriggerGroupNames()) {
                for (TriggerKey key : scheduler.getTriggerKeys(GroupMatcher.triggerGroupEquals(group))) {
                    Trigger trigger = scheduler.getTrigger(key);
                    if (trigger == null) {
                        continue;
                    }
                    Map<String, Object> tm = new LinkedHashMap<>();
                    tm.put("trigger", key.getName());
                    tm.put("job", trigger.getJobKey().getName());
                    tm.put("state", scheduler.getTriggerState(key).name());
                    tm.put("previousFireTime", instant(trigger.getPreviousFireTime()));
                    tm.put("nextFireTime", instant(trigger.getNextFireTime()));
                    list.add(tm);
                }
            }
        } catch (SchedulerException e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", e.getMessage());
            list.add(err);
        }
        return list;
    }

    private static String instant(Date date) {
        return date == null ? null : date.toInstant().toString();
    }
}