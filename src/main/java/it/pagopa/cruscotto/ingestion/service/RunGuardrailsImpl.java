package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;

@Slf4j
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
                if (elapsed.compareTo(guardrailsConfig.getMaxDuration()) > 0) {
                    LogHelper.warn(ctx, RunPhase.SKIP,
                        "Max duration exceeded: " + elapsed + " > " + guardrailsConfig.getMaxDuration());
                    return false;
                }
            }
        }

        // Check max queries
        if (guardrailsConfig.isEnableMaxQueries()) {
            if (queriesExecuted >= guardrailsConfig.getMaxQueries()) {
                LogHelper.warn(ctx, RunPhase.SKIP,
                    "Max queries exceeded: " + queriesExecuted + " >= " + guardrailsConfig.getMaxQueries());
                return false;
            }
        }

        // Check max rows
        if (guardrailsConfig.isEnableMaxRows()) {
            if (rowsProcessed >= guardrailsConfig.getMaxRows()) {
                LogHelper.warn(ctx, RunPhase.SKIP,
                    "Max rows exceeded: " + rowsProcessed + " >= " + guardrailsConfig.getMaxRows());
                return false;
            }
        }

        return true;
    }
}


