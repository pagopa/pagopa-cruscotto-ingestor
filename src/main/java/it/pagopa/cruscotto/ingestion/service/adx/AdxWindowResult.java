package it.pagopa.cruscotto.ingestion.service.adx;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

@Data
@AllArgsConstructor
public class AdxWindowResult {
    private Instant fromInclusive;
    private Instant toExclusive;
    private Duration windowUsed;
    private int attempts;
    private Map<String, Object> rows;
}

