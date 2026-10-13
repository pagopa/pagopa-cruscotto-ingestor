package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class RunGuardrailsImpl implements RunGuardrails {

    private final IngestionConfig ingestionConfig;

    @Override
    public boolean ok(RunContext ctx, long queriesExecuted, long rowsProcessed) {
        IngestionConfig.GuardrailsConfig guardrailsConfig = ingestionConfig.getGuardrails();

        // Check max duration
        if (guardrailsConfig.isEnableMaxDuration()) {
            Instant runStartTime = ctx.getRunStart();
            if (runStartTime != null) {
                Duration elapsed = Duration.between(runStartTime, Instant.now());
                Duration maxDuration = ingestionConfig.resolveMaxDurationForRun(ctx.getEntityName(), ctx.isCatchupMode());
                if (elapsed.compareTo(maxDuration) > 0) {
                    try (LogDetails ignored = LogDetails.open()
                            .with("elapsed", elapsed)
                            .with("max_duration", maxDuration)) {
                        LogHelper.warn(ctx, RunPhase.SKIP, "Stopped run on the max duration guardrail");
                    }
                    return false;
                }
            }
        }

        // Check max queries
        if (guardrailsConfig.isEnableMaxQueries()) {
            if (queriesExecuted >= guardrailsConfig.getMaxQueries()) {
                try (LogDetails ignored = LogDetails.open()
                        .with(LogFields.QUERIES_EXECUTED, queriesExecuted)
                        .with("max_queries", guardrailsConfig.getMaxQueries())) {
                    LogHelper.warn(ctx, RunPhase.SKIP, "Stopped run on the max queries guardrail");
                }
                return false;
            }
        }

        // Check max rows
        if (guardrailsConfig.isEnableMaxRows()) {
            if (rowsProcessed >= guardrailsConfig.getMaxRows()) {
                try (LogDetails ignored = LogDetails.open()
                        .with(LogFields.ROWS_PROCESSED, rowsProcessed)
                        .with("max_rows", guardrailsConfig.getMaxRows())) {
                    LogHelper.warn(ctx, RunPhase.SKIP, "Stopped run on the max rows guardrail");
                }
                return false;
            }
        }

        return true;
    }
}

