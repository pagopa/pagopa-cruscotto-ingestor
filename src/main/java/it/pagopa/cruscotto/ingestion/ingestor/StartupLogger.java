package it.pagopa.cruscotto.ingestion.ingestor;

import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * Emits a single, unique, greppable marker at JVM start and graceful stop so that app restarts can be
 * spotted with one query in Elastic/Kibana:
 *
 * <ul>
 *   <li>{@code APP_STARTUP} — logged once when the context is ready. Each occurrence = one JVM (re)start.</li>
 *   <li>{@code APP_SHUTDOWN} — logged once on a graceful stop (SIGTERM / context close). It does NOT
 *       fire on an abrupt death (OOM-kill / SIGKILL / node loss).</li>
 * </ul>
 *
 * <p>Ops rule of thumb: an {@code APP_STARTUP} not preceded by an {@code APP_SHUTDOWN} for the same
 * {@code instanceId} means the previous process died abruptly (OOM / kill) — check the Kubernetes pod
 * events. The {@code instanceId} is the same pod identifier recorded in {@code INGEST_EXECUTION_LOG}.</p>
 */
@Slf4j
@Component
public class StartupLogger {

    private final Environment environment;

    public StartupLogger(Environment environment) {
        this.environment = environment;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        long maxHeapMb = Runtime.getRuntime().maxMemory() / (1024L * 1024L);
        String profiles = String.join(",", environment.getActiveProfiles());
        log.info("APP_STARTUP instanceId={} pid={} activeProfiles={} maxHeapMb={} — application ready"
                        + " (each APP_STARTUP marks a JVM (re)start)",
                ExecutionLogService.getInstanceId(), ProcessHandle.current().pid(), profiles, maxHeapMb);
    }

    @EventListener(ContextClosedEvent.class)
    public void onContextClosed() {
        log.info("APP_SHUTDOWN instanceId={} pid={} — graceful shutdown"
                        + " (absence of APP_SHUTDOWN before the next APP_STARTUP = abrupt death: OOM / SIGKILL)",
                ExecutionLogService.getInstanceId(), ProcessHandle.current().pid());
    }
}
