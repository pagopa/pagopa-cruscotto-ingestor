package it.pagopa.cruscotto.ingestion.massivesearch.facade;

import java.util.UUID;

/**
 * Internal entry point of the Massive Search bounded context, invoked by the external API Layer.
 *
 * <p>Implementations orchestrate a search execution (Perimeter CSV when needed, the three report
 * CSVs and the result ZIP) for a given instance. Only the latest result per instance is kept.
 */
public interface MassiveSearchFacade {

    /**
     * Starts the first execution of the given search instance.
     *
     * @param instanceId identifier of the Massive Search instance
     * @return the start outcome of the execution
     */
    SearchExecutionStartResult execute(UUID instanceId);

    /**
     * Re-runs the given search instance, replacing its previous (latest) result.
     *
     * @param instanceId identifier of the Massive Search instance
     * @return the start outcome of the execution
     */
    SearchExecutionStartResult rerun(UUID instanceId);
}
