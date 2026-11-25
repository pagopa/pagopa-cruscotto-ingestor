package it.pagopa.cruscotto.ingestion.fdr.workflow;

/**
 * I tre modi in cui una giornata FDR finisce per essere elaborata.
 *
 * <p>Tutti e tre convergono sullo stesso {@link FdrDayProcessor}: cambia chi decide le giornate e
 * cosa succede prima, non come la giornata viene elaborata.
 */
public enum FdrWorkflow {

    /** Creato dallo scheduler dell'Ingestor, una giornata per volta, quella di ieri. */
    DAILY,

    /**
     * Richiesto dal backend su un intervallo di giornate.
     *
     * <p>L'intervallo lo espande l'Ingestor dopo il claim: il backend dice "dal 1 al 15", e la
     * granularita' per dataset resta un dettaglio interno nostro. E' l'asimmetria voluta rispetto
     * alla REINGESTION, dove le giornate devono invece essere esplicite perche' possono essere
     * sparse.
     */
    BOOTSTRAP,

    /**
     * Richiesto dal backend su un insieme di giornate, anche non contigue.
     *
     * <p>Le giornate sono quelle degli status associati al batch, <strong>mai</strong>
     * {@code REQUESTED_FROM..REQUESTED_TO}: su una richiesta sparsa quell'intervallo
     * conterrebbe giornate che nessuno ha chiesto.
     */
    REINGESTION
}
