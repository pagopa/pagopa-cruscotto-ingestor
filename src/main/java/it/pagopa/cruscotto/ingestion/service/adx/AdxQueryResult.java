package it.pagopa.cruscotto.ingestion.service.adx;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;

@Data
@AllArgsConstructor
public class AdxQueryResult {
    private boolean success;
    private Map<String, Object> data;
    private String error;
}

