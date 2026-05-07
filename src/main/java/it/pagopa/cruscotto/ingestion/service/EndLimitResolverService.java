package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class EndLimitResolverService {
    private final CheckpointStoreService checkpointStoreService;

    /**
     * Resolves the end limit for processing based on parent checkpoints.
     * Rules:
     * - POSITION: endLimit = now() - 2 hours
     * - POSITION_TOKENS: endLimit = checkpoint(POSITION) (if missing -> NOOP)
     * - POSITION_TRANSFERS: endLimit = checkpoint(POSITION_TOKENS) (if missing -> NOOP)
     * - EVENTS_WF: endLimit = checkpoint(POSITION_TRANSFERS) (if missing -> NOOP)
     * - EXTRA_INFO: endLimit = checkpoint(EVENTS_WF) (if missing -> NOOP)
     *
     * @param ctx the run context containing entityName and runId
     * @return Optional containing the end limit timestamp, or empty if NOOP
     */
    public Optional<Instant> resolveEndLimit(RunContext ctx) {
        EntityName entityName;
        try {
            entityName = EntityName.valueOf(ctx.getEntityName());
        } catch (IllegalArgumentException ex) {
            log.error("UNKNOWN_ENTITY runId={} entityName={}", ctx.getRunId(), ctx.getEntityName());
            return Optional.empty();
        }

        String runId = ctx.getRunId();

        switch (entityName) {
            case POSITION:
                Instant endLimit = Instant.now().minusSeconds(7200); // now() - 2 hours
                log.info("END_LIMIT runId={} entityName={} endLimit={}", runId, entityName.name(), endLimit);
                return Optional.of(endLimit);

            case POSITION_TOKENS:
                return resolveFromParentCheckpoint(ctx, EntityName.POSITION);

            case POSITION_TRANSFERS:
                return resolveFromParentCheckpoint(ctx, EntityName.POSITION_TOKENS);

            case EXTRA_INFO:
                return resolveFromParentCheckpoint(ctx, EntityName.EVENTS_WF);

            case EVENTS_WF:
                return resolveFromParentCheckpoint(ctx, EntityName.POSITION_TRANSFERS);

            default:
                log.error("UNKNOWN_ENTITY runId={} entityName={}", runId, entityName.name());
                return Optional.empty();
        }
    }

    private Optional<Instant> resolveFromParentCheckpoint(RunContext ctx, EntityName parentEntity) {
        String entityName = ctx.getEntityName();
        String runId = ctx.getRunId();

        Optional<Instant> parentCheckpoint = checkpointStoreService.getCheckpoint(parentEntity);
        if (parentCheckpoint.isPresent()) {
            log.info("END_LIMIT runId={} entityName={} parentEntity={} parentCheckpoint={}",
                    runId, entityName, parentEntity.name(), parentCheckpoint.get());
            return parentCheckpoint;
        } else {
            log.info("NOOP runId={} entityName={} missingParentCheckpoint parentEntity={}",
                    runId, entityName, parentEntity.name());
            return Optional.empty();
        }
    }
}
