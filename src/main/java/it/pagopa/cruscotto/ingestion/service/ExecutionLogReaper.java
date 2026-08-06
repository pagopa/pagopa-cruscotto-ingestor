package it.pagopa.cruscotto.ingestion.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Periodically closes runs stuck in STARTED that were interrupted (OOM / pod restart / SIGKILL)
 * before they could log their own END. Marks them ABORTED with a diagnostic reason via
 * {@link ExecutionLogService#markOrphanedRunsAborted(Duration)} so the execution-log table alone
 * tells the full story (no need to cross-check Elastic to notice a run died).
 * <p>
 * Runs on every pod; the underlying UPDATE only matches STATUS='STARTED' rows older than the
 * threshold, so it is idempotent and safe under concurrent execution.
 */
@Slf4j
@Component
public class ExecutionLogReaper {

    private final ExecutionLogService executionLogService;

    /**
     * A STARTED run with no heartbeat/END for longer than this is considered dead. MUST stay well
     * above the largest guardrail max-duration (a live run can never exceed it) and above the worst
     * single-window duration (ADX socket timeout + halving/retries), so a slow-but-alive run is
     * never reaped. Default 15m » 25m? No: guardrail is 25m total but the heartbeat fires every
     * window, so the gap between heartbeats is at most one slow window (~6-10m); 15m is safe.
     */
    @Value("${ingestion.executionLog.orphan-threshold:15m}")
    private Duration orphanThreshold;

    public ExecutionLogReaper(ExecutionLogService executionLogService) {
        this.executionLogService = executionLogService;
    }

    @Scheduled(
            fixedDelayString = "${ingestion.executionLog.reaper-interval-ms:120000}",
            initialDelayString = "${ingestion.executionLog.reaper-interval-ms:120000}")
    public void reapOrphanedRuns() {
        try {
            executionLogService.markOrphanedRunsAborted(orphanThreshold);
        } catch (Exception ex) {
            log.warn("[phase=REAP_ORPHANED_RUNS] reaper failed: {}", ex.getMessage());
        }
    }
}
