package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Provides configuration values for ADX query builders.
 */
@Component
@RequiredArgsConstructor
public class IngestionConfigProvider {
    private final IngestionConfig ingestionConfig;

    /**
     * Return true if size estimate statistics should be appended to queries.
     * Configured via ingestion.adx.include-estimates property.
     */
    public boolean isIncludeEstimates() {
        return ingestionConfig.getAdx().isIncludeEstimates();
    }
}


