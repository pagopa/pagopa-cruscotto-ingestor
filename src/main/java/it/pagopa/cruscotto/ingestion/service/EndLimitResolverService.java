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
    public static final String REASON_RESOLVED = "RESOLVED";
    public static final String REASON_UNKNOWN_ENTITY = "UNKNOWN_ENTITY";
    public static final String REASON_MISSING_PARENT_CHECKPOINT_PREFIX = "MISSING_PARENT_CHECKPOINT_";

    private final CheckpointStoreService checkpointStoreService;

    public record EndLimitResolution(Optional<Instant> endLimit, String reason) {
    }

    /**
     * Resolves the end limit for processing based on parent checkpoints.
     * Rules:
     * - POSITION: endLimit = now() - 2 hours
     * - POSITION_TOKENS: endLimit = checkpoint(POSITION) (if missing -> NOOP)
     * - POSITION_TRANSFERS: endLimit = checkpoint(POSITION_TOKENS) (if missing -> NOOP)
     * - EVENTS_WF: endLimit = checkpoint(POSITION_TOKENS) (if missing -> NOOP)
     * - EXTRA_INFO: endLimit = checkpoint(POSITION_TOKENS) (if missing -> NOOP)
     *
     * @param ctx the run context containing entityName and runId
     * @return Optional containing the end limit timestamp, or empty if NOOP
     */
    public Optional<Instant> resolveEndLimit(RunContext ctx) {
        return resolveEndLimitDetailed(ctx).endLimit();
    }

    public EndLimitResolution resolveEndLimitDetailed(RunContext ctx) {
        EntityName entityName;
        try {
            entityName = EntityName.valueOf(ctx.getEntityName());
        } catch (IllegalArgumentException ex) {
            log.error("UNKNOWN_ENTITY runId={} entityName={}", ctx.getRunId(), ctx.getEntityName());
            return new EndLimitResolution(Optional.empty(), REASON_UNKNOWN_ENTITY);
        }

        String runId = ctx.getRunId();

        switch (entityName) {
            case POSITION:
                Instant endLimit = Instant.now().minusSeconds(7200); // now() - 2 hours
                log.info("END_LIMIT runId={} entityName={} endLimit={}", runId, entityName.name(), endLimit);
                return new EndLimitResolution(Optional.of(endLimit), REASON_RESOLVED);

            case POSITION_TOKENS:
                return resolveFromParentCheckpoint(ctx, EntityName.POSITION);

            case POSITION_TRANSFERS:
                return resolveFromParentCheckpoint(ctx, EntityName.POSITION_TOKENS);

            case EXTRA_INFO:
                return resolveFromParentCheckpoint(ctx, EntityName.POSITION_TOKENS);

            case EVENTS_WF:
                return resolveFromParentCheckpoint(ctx, EntityName.POSITION_TOKENS);

            default:
                log.error("UNKNOWN_ENTITY runId={} entityName={}", runId, entityName.name());
                return new EndLimitResolution(Optional.empty(), REASON_UNKNOWN_ENTITY);
        }
    }

    private EndLimitResolution resolveFromParentCheckpoint(RunContext ctx, EntityName parentEntity) {
        String entityName = ctx.getEntityName();
        String runId = ctx.getRunId();

        Optional<Instant> parentCheckpoint = checkpointStoreService.getCheckpoint(parentEntity);
        if (parentCheckpoint.isPresent()) {
            log.info("END_LIMIT runId={} entityName={} parentEntity={} parentCheckpoint={}",
                    runId, entityName, parentEntity.name(), parentCheckpoint.get());
            return new EndLimitResolution(parentCheckpoint, REASON_RESOLVED);
        } else {
            String reason = REASON_MISSING_PARENT_CHECKPOINT_PREFIX + parentEntity.name();
            log.info("NOOP runId={} entityName={} missingParentCheckpoint parentEntity={}",
                    runId, entityName, parentEntity.name());
            return new EndLimitResolution(Optional.empty(), reason);
        }
    }
}
