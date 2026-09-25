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
 * usable), exact match on TOKEN against its stored UTF-8 bytes, parameter binding, invalid-key skipping and the null contract.
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
    void iuvResolvesPositionsThroughTokensTable() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.IUV, SCHEMA,
            List.of(row(null, null, "abcIUV", null)), params);

        // La risoluzione parte da position_tokens (indice su iuv) e non da una EXISTS correlata su p.id,
        // che lasciava position come tabella guida.
        assertTrue(clause.contains("SELECT DISTINCT tkf.fk_position"), clause);
        assertTrue(clause.contains(SCHEMA + ".position_tokens tkf"), clause);
        assertTrue(clause.contains("tkf.iuv = kv.iuv"), clause);
        assertTrue(clause.contains("k ON p.id = k.fk_position"), clause);
        assertFalse(clause.contains("EXISTS"), clause);
        assertNoLower(clause);
        assertEquals("abcIUV", params.getValue("kj_0"));
    }

    @Test
    void iuvPaCombinesTokenResolutionWithExactPa() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.IUV_PA, SCHEMA,
            List.of(row(null, "77777777777", "abcIUV", null)), params);

        assertTrue(clause.contains("AS kv(pa, iuv)"), clause);
        assertTrue(clause.contains("tkf.iuv = kv.iuv"), clause);
        assertTrue(clause.contains("p.id = k.fk_position AND p.pa_emittente = k.pa"), clause);
        assertFalse(clause.contains("EXISTS"), clause);
        assertNoLower(clause);
    }

    @Test
    void tokenMatchesTheStoredUtf8BytesNotAHexDecoding() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.TOKEN, SCHEMA,
            List.of(row(null, null, null, "deadbeef")), params);

        // position_tokens.token e' BYTEA ma contiene i byte UTF-8 della stringa di origine
        // (ingestion: getBytes(UTF_8); report: convert_from(token,'UTF8')). Una decode('hex')
        // confronterebbe byte diversi e non troverebbe mai nulla.
        assertTrue(clause.contains("tkf.token = convert_to(kv.token, 'UTF8')"), clause);
        assertFalse(clause.contains("decode("), clause);
        assertEquals("deadbeef", params.getValue("kj_0"));
    }

    @Test
    void tokenWithNonHexCharactersIsBoundVerbatim() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.TOKEN, SCHEMA,
            List.of(row(null, null, null, "zz-not-hex-01")), params);

        // Con decode('hex') questo valore faceva fallire l'intera query in runtime.
        assertTrue(clause.contains("tkf.token = convert_to(kv.token, 'UTF8')"), clause);
        assertEquals("zz-not-hex-01", params.getValue("kj_0"));
    }

    @Test
    void tokenResolutionKeepsTheKeyColumnSoCardinalityMatchesTheFormerExists() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.TOKEN, SCHEMA,
            List.of(row(null, null, null, "deadbeef")), params);

        // La chiave resta nella proiezione: una riga per ogni coppia (posizione, chiave), come la
        // semi-join EXISTS precedente. Senza la chiave due token distinti della stessa posizione
        // collasserebbero in una riga sola, cambiando il numero di righe del report.
        assertTrue(clause.contains("SELECT DISTINCT tkf.fk_position AS fk_position, kv.token AS token"), clause);
    }

    @Test
    void keyResolutionNeverFiltersOnDateEvent() {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String clause = ReportKeyJoinSql.buildKeyJoin(CsvTemplate.IUV, SCHEMA,
            List.of(row(null, null, "abcIUV", null)), params);

        // date_event e' riscritto a ogni update con il giorno dell'ultimo evento, mentre la finestra
        // e' su inserted_timestamp (primo evento): potare su date_event escluderebbe i token pagati
        // dopo la fine della finestra.
        assertFalse(clause.contains("date_event"), clause);
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
