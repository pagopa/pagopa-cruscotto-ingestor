package it.pagopa.cruscotto.ingestion.ingestor;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogHelper {
    private static final Logger log = LoggerFactory.getLogger(LogHelper.class);

    public static void info(RunContext ctx, RunPhase phase, String message, Object... args) {
        log.info(format(ctx, phase.name(), message), args);
    }

    public static void info(RunContext ctx, String phase, String message, Object... args) {
        log.info(format(ctx, phase, message), args);
    }

    public static void warn(RunContext ctx, RunPhase phase, String message, Object... args) {
        log.warn(format(ctx, phase.name(), message), args);
    }

    public static void warn(RunContext ctx, String phase, String message, Object... args) {
        log.warn(format(ctx, phase, message), args);
    }

    public static void error(RunContext ctx, RunPhase phase, String message, Object... args) {
        log.error(format(ctx, phase.name(), message), args);
    }

    public static void error(RunContext ctx, String phase, String message, Object... args) {
        log.error(format(ctx, phase, message), args);
    }

    private static String format(RunContext ctx, String phase, String message) {
        return String.format("[runId=%s][entity=%s][phase=%s] %s", ctx.getRunId(), ctx.getEntityName(), phase, message);
    }
}
