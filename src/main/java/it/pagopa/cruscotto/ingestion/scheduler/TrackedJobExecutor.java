package it.pagopa.cruscotto.ingestion.scheduler;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Wraps the body of a Quartz job so that <b>every</b> failure is recorded in
 * {@code INGEST_EXECUTION_LOG}, not only in the application log.
 *
 * <p>Rationale: production has no direct log access, so the execution-log table must be
 * self-sufficient for diagnosis. Without this wrapper two classes of failure were invisible there:
 * jobs that never write an execution-log row at all (cleanup jobs, reconciliation), and failures
 * that happen <em>before</em> a runner can create its row (job launch, Spring Batch, DB unreachable).</p>
 *
 * <p>Two modes, so a job never ends up with duplicate {@code STARTED} rows:</p>
 * <ul>
 *   <li>{@link #runTracked} — owns the full lifecycle (STARTED → COMPLETED/FAILED). For jobs whose
 *       body does not write the execution log itself (cleanup jobs, reconciliation).</li>
 *   <li>{@link #recordFailure} — records only the failure, called from the job's existing
 *       {@code catch}. For jobs whose runner already owns the lifecycle: {@code logFailed} upserts,
 *       so it updates the runner's row when present and inserts a FAILED row when the job died
 *       before that row existed.</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TrackedJobExecutor {

    /** Depth of the cause chain kept in ERROR_MESSAGE: enough to diagnose without the app log. */
    private static final int MAX_CAUSE_DEPTH = 5;

    private final ExecutionLogService executionLogService;

    /** Body of a Quartz job; may throw anything, which is recorded and rethrown. */
    @FunctionalInterface
    public interface JobBody {
        void run() throws Exception;
    }

    /**
     * Runs the body owning the whole execution-log lifecycle: a STARTED row on entry, then
     * COMPLETED or FAILED. Use for jobs that do not write the execution log themselves.
     */
    public void runTracked(String entityName, String jobName, String runId, JobBody body)
            throws JobExecutionException {
        RunContext ctx = new RunContext(entityName, runId, Instant.now());
        executionLogService.logStarted(ctx, jobName);
        try {
            body.run();
        } catch (Throwable t) {
            recordFailure(ctx, jobName, t);
            throw new JobExecutionException(t);
        }
        executionLogService.logCompleted(ctx, 0, 0, 0, 0, 0, 0, 1, "COMPLETED");
    }

    /**
     * Records a job failure in the execution log, leaving the lifecycle to the runner. Meant to be
     * called from the job's existing {@code catch} block before rethrowing: it upserts, so it
     * updates the runner's row when present and inserts a FAILED row when the job died before it.
     */
    public void recordFailure(String entityName, String jobName, String runId, Throwable t) {
        recordFailure(new RunContext(entityName, runId, Instant.now()), jobName, t);
    }

    private void recordFailure(RunContext ctx, String jobName, Throwable t) {
        String detail = describe(t);
        log.error("jobTag={} ERROR runId={} entityName={} error={}",
                jobName, ctx.getRunId(), ctx.getEntityName(), detail, t);
        // Never let the bookkeeping hide the original failure, which is rethrown by the caller.
        try {
            // Use the ROOT cause for ERROR_CODE: it is the actionable class (e.g.
            // DataIntegrityViolationException), not the generic RuntimeException/JobExecutionException
            // wrapper. Also keeps the code precise if a self-logging runner's row is later overwritten
            // here after it rethrew a wrapped exception.
            executionLogService.logFailed(ctx, jobName, rootCauseName(t), detail,
                    0, 0, 0, 0, 0, 0, 0);
        } catch (Exception loggingFailure) {
            log.error("jobTag={} ERROR runId={} entityName={} unable to record the failure in the execution log: {}",
                    jobName, ctx.getRunId(), ctx.getEntityName(), loggingFailure.toString());
        }
    }

    /**
     * Renders the exception with its cause chain: the root cause is usually the actionable part
     * (e.g. the Postgres or ADX message) and it must be readable straight from the table.
     */
    private String describe(Throwable t) {
        StringBuilder sb = new StringBuilder();
        Throwable current = t;
        int depth = 0;
        while (current != null && depth < MAX_CAUSE_DEPTH) {
            if (depth > 0) {
                sb.append(" | causedBy=");
            }
            sb.append(current.getClass().getSimpleName());
            if (current.getMessage() != null) {
                sb.append(": ").append(current.getMessage());
            }
            current = current.getCause() == current ? null : current.getCause();
            depth++;
        }
        return sb.toString();
    }

    /** Simple name of the deepest cause: the actionable class, not the wrapper. */
    private String rootCauseName(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName();
    }
}
