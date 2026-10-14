package it.pagopa.cruscotto.ingestion.ingestor;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
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
        try (MdcScope ignored = MdcScope.phase("APP_STARTUP")
                     .with(LogFields.CTX_INSTANCE_ID, ExecutionLogService.getInstanceId());
             LogDetails details = LogDetails.open()
                     .with("pid", ProcessHandle.current().pid())
                     .with("active_profiles", profiles)
                     .with("max_heap_mb", maxHeapMb)) {
            // Ogni riga di avvio segna un (ri)avvio della JVM.
            log.info("Application ready");
        }
    }

    @EventListener(ContextClosedEvent.class)
    public void onContextClosed() {
        try (MdcScope ignored = MdcScope.phase("APP_SHUTDOWN")
                     .with(LogFields.CTX_INSTANCE_ID, ExecutionLogService.getInstanceId());
             LogDetails details = LogDetails.open()
                     .with("pid", ProcessHandle.current().pid())) {
            // L assenza di questa riga prima del prossimo avvio significa morte brusca: OOM o SIGKILL.
            log.info("Graceful shutdown");
        }
    }
}
