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

    public static void debug(RunContext ctx, RunPhase phase, String message, Object... args) {
        log.debug(format(ctx, phase.name(), message), args);
    }

    public static void debug(RunContext ctx, String phase, String message, Object... args) {
        log.debug(format(ctx, phase, message), args);
    }

    public static void error(RunContext ctx, RunPhase phase, String message, Object... args) {
        log.error(format(ctx, phase.name(), message), args);
    }

    public static void error(RunContext ctx, String phase, String message, Object... args) {
        log.error(format(ctx, phase, message), args);
    }

    private static String format(RunContext ctx, String phase, String message) {
        String jobTag = resolveJobTag(ctx.getEntityName());
        if (ctx.getOperationId() != null && !ctx.getOperationId().isBlank()) {
            return String.format("[jobTag=%s][runId=%s][operationId=%s][entity=%s][phase=%s] %s",
                    jobTag, ctx.getRunId(), ctx.getOperationId(), ctx.getEntityName(), phase, message);
        }
        return String.format("[jobTag=%s][runId=%s][entity=%s][phase=%s] %s",
                jobTag, ctx.getRunId(), ctx.getEntityName(), phase, message);
    }

    private static String resolveJobTag(String entityName) {
        if (entityName == null || entityName.isBlank()) {
            return "unknownJob";
        }
        return switch (entityName) {
            case "POSITION" -> "positionJob";
            case "POSITION_TOKENS" -> "positionTokensJob";
            case "POSITION_TRANSFERS" -> "positionTransfersJob";
            case "EXTRA_INFO" -> "extraInfoJob";
            case "EVENTS_WF" -> "eventsWfJob";
            case "RECONCILIATION" -> "reconciliationJob";
            default -> entityName.toLowerCase() + "Job";
        };
    }
}
