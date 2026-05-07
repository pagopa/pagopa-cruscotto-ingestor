package it.pagopa.cruscotto.ingestion.service.ingestion;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;

@Data
@AllArgsConstructor
public class BulkWriteResult {
    private int rowsInserted;
    private Instant maxInsertedTimestamp;
}

