package it.pagopa.cruscotto.ingestion.service.ingestion;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchLocalCacheTest {

    @Test
    void shouldReplaceSyntheticPositionIdWhenDatabaseIdIsCached() {
        BatchLocalCache cache = new BatchLocalCache();
        LocalDateTime insertedTimestamp = LocalDateTime.of(2026, 7, 17, 10, 0);

        cache.cachePosition(-1, "NAV-1", "PA-1", insertedTimestamp);
        cache.cachePosition(42, "NAV-1", "PA-1", insertedTimestamp);

        assertEquals(42, cache.findPositionInWindow("NAV-1", "PA-1", insertedTimestamp.plusMinutes(1)));
    }
}
