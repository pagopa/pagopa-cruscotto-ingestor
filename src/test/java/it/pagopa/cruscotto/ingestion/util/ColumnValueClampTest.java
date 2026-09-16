package it.pagopa.cruscotto.ingestion.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ColumnValueClampTest {

    @Test
    void returnsNullUnchanged() {
        assertNull(ColumnValueClamp.clamp(null, 255));
    }

    @Test
    void returnsSameInstanceWhenWithinLimit() {
        String value = "0123456789";
        assertSame(value, ColumnValueClamp.clamp(value, 255));
    }

    @Test
    void returnsSameInstanceWhenExactlyAtLimit() {
        String value = "x".repeat(255);
        assertSame(value, ColumnValueClamp.clamp(value, ColumnValueClamp.VARCHAR_255));
    }

    @Test
    void truncatesToLimitWhenOversized() {
        String value = "a".repeat(400);
        String clamped = ColumnValueClamp.clamp(value, ColumnValueClamp.VARCHAR_255);
        assertEquals(255, clamped.length());
        assertEquals(value.substring(0, 255), clamped);
    }

    @Test
    void neverSplitsSurrogatePair() {
        // 254 ASCII chars + one emoji (a surrogate pair) => the pair straddles the 255-code-unit
        // boundary (positions 254 and 255). The clamp must drop the whole pair, not leave a lone
        // high surrogate that PostgreSQL would reject as invalid UTF-8.
        String value = "a".repeat(254) + "😀" + "b".repeat(200);
        String clamped = ColumnValueClamp.clamp(value, ColumnValueClamp.VARCHAR_255);
        assertEquals(254, clamped.length());
        assertEquals("a".repeat(254), clamped);
    }
}
