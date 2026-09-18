package it.pagopa.cruscotto.ingestion.massivesearch.report;

import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvTemplate;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.SearchInputRow;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the key-join SQL: exact equality on NAV/PA/IUV (no LOWER, so the plain b-tree indexes are
 * usable), decode('hex') on TOKEN, parameter binding, invalid-key skipping and the null contract.
 */
class ReportKeyJoinSqlTest {

    private static final String SCHEMA = "sert_ingestor";

    private SearchInputRow row(String nav, String pa, String iuv, String token) {
        return new SearchInputRow(nav, pa, iuv, token);
    }

    @Test
    void navPaUsesExactEqualityOnBothColumns() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.NAV_PA, SCHEMA,
            List.of(row("001", "77777777777", null, null)), params);

        assertTrue(clause.contains("AS k(nav, pa)"), clause);
        assertTrue(clause.contains("p.nav = k.nav AND p.pa_emittente = k.pa"), clause);
        assertNoLower(clause);
        assertEquals("001", params.getValue("kj_0_0"));
        assertEquals("77777777777", params.getValue("kj_0_1"));
    }

    @Test
    void navUsesExactEquality() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.NAV, SCHEMA,
            List.of(row("001", null, null, null)), params);

        assertTrue(clause.contains("AS k(nav)"), clause);
        assertTrue(clause.contains("p.nav = k.nav"), clause);
        assertNoLower(clause);
        assertEquals("001", params.getValue("kj_0"));
    }

    @Test
    void iuvUsesCaseSensitiveExistsOnTokens() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.IUV, SCHEMA,
            List.of(row(null, null, "abcIUV", null)), params);

        assertTrue(clause.contains(SCHEMA + ".position_tokens tkf"), clause);
        assertTrue(clause.contains("tkf.fk_position = p.id AND tkf.iuv = k.iuv"), clause);
        assertNoLower(clause);
        assertEquals("abcIUV", params.getValue("kj_0"));
    }

    @Test
    void iuvPaCombinesExactPaWithTokenExists() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.IUV_PA, SCHEMA,
            List.of(row(null, "77777777777", "abcIUV", null)), params);

        assertTrue(clause.contains("AS k(pa, iuv)"), clause);
        assertTrue(clause.contains("p.pa_emittente = k.pa AND EXISTS"), clause);
        assertTrue(clause.contains("tkf.iuv = k.iuv"), clause);
        assertNoLower(clause);
    }

    @Test
    void tokenUsesHexDecode() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.TOKEN, SCHEMA,
            List.of(row(null, null, null, "deadbeef")), params);

        assertTrue(clause.contains("tkf.token = decode(k.token, 'hex')"), clause);
        assertEquals("deadbeef", params.getValue("kj_0"));
    }

    @Test
    void skipsKeysMissingRequiredFieldsAndReturnsNullWhenNoneValid() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        // NAV_PA needs both nav and pa; this key has no pa -> skipped -> no valid key -> null.
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.NAV_PA, SCHEMA,
            List.of(row("001", null, null, null)), params);
        assertNull(clause);
    }

    @Test
    void returnsNullForUnknownTemplate() {
        assertNull(ReportKeyJoinSql.buildKeyJoin(CsvTemplate.UNKNOWN, SCHEMA,
            List.of(row("001", "77777777777", null, null)), new MapSqlParameterSource()));
    }

    private static void assertNoLower(String clause) {
        assertFalse(clause.toUpperCase().contains("LOWER("), "clause must not wrap indexed columns in LOWER(): " + clause);
    }
}