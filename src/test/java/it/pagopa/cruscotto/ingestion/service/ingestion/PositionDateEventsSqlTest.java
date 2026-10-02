package it.pagopa.cruscotto.ingestion.service.ingestion;

import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Blinda il contratto dell'UPDATE condivisa su DATE_EVENTS.
 *
 * <p>Il comportamento end-to-end richiede un PostgreSQL reale (non disponibile come dipendenza di
 * test in questo progetto), quindi qui si verificano le proprieta' strutturali che, se perse,
 * reintrodurrebbero i difetti gia' occorsi in produzione: riscrittura delle coordinate di nascita,
 * batch abortito da un elemento malformato, array non deduplicato.</p>
 */
class PositionDateEventsSqlTest {

    private final String sql = PositionDateEventsSql.appendDateEvent("ingestor");

    @Test
    void neverWritesTheBirthCoordinates() {
        assertTrue(sql.startsWith("UPDATE ingestor.POSITION SET"), sql);
        assertFalse(sql.contains("DATE_EVENT = ?"), sql);
        assertFalse(sql.contains("INSERTED_TIMESTAMP = ?"), sql);
        assertFalse(sql.contains("NAV = ?"), sql);
        assertFalse(sql.contains("PA_EMITTENTE = ?"), sql);
    }

    @Test
    void keepsLastEventMonotonic() {
        assertTrue(sql.contains("LAST_EVENT = GREATEST(LAST_EVENT, ?::timestamp)"), sql);
    }

    @Test
    void neverCastsArrayElementsToDate() {
        // Un cast a date fallisce su valori con forma valida ma data inesistente (es. "20261340") e
        // in un batch l'errore aborta l'intera transazione, non solo la riga sporca.
        assertFalse(sql.contains("::date"), sql);
        assertTrue(sql.contains("jsonb_typeof(DATE_EVENTS) = 'array'"),
                "un DATE_EVENTS non-array farebbe fallire jsonb_array_elements_text: " + sql);
    }

    @Test
    void deduplicatesSortsAndExcludesTheBirthDay() {
        assertTrue(sql.contains("jsonb_agg(DISTINCT d ORDER BY d)"), sql);
        assertTrue(sql.contains("d <> to_char(DATE_EVENT, 'YYYY-MM-DD')"), sql);
    }

    @Test
    void bindsExactlyFourParameters() {
        assertEquals(4, sql.chars().filter(c -> c == '?').count(), sql);
    }

    @Test
    void validationPatternsRejectImpossibleDatesAndAcceptBothFormats() {
        // Gli stessi pattern applicati lato DB: si verifica qui che filtrino cio' che il cast
        // avrebbe rifiutato lanciando, e che il formato legacy resti riconosciuto.
        Pattern iso = extractPattern("^\\^\\[0-9\\]\\{4\\}-.*?\\$");
        Pattern basicIso = extractPattern("^\\^\\[0-9\\]\\{4\\}\\(.*?\\$");

        assertTrue(iso.matcher("2026-09-11").matches());
        assertFalse(iso.matcher("2026-13-01").matches());
        assertFalse(iso.matcher("2026-09-40").matches());
        assertFalse(iso.matcher("not-a-date").matches());

        assertTrue(basicIso.matcher("20260911").matches());
        assertFalse(basicIso.matcher("20261340").matches());
        assertFalse(basicIso.matcher("00000000").matches());
    }

    /** Estrae dal testo SQL i pattern realmente usati, così il test resta legato all'implementazione. */
    private Pattern extractPattern(String locator) {
        java.util.regex.Matcher matcher = Pattern.compile("'(\\^\\[0-9\\]\\{4\\}[^']*\\$)'").matcher(sql);
        while (matcher.find()) {
            String candidate = matcher.group(1);
            if (Pattern.compile(locator).matcher(candidate).find()) {
                return Pattern.compile(candidate);
            }
        }
        throw new AssertionError("pattern non trovato nella SQL per locator=" + locator + " sql=" + sql);
    }
}
