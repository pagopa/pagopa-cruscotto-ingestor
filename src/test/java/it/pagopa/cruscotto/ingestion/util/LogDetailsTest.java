package it.pagopa.cruscotto.ingestion.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Il contratto di {@link LogDetails} e di {@link MdcScope#run}.
 *
 * <p>{@link CtxDetailsJsonCustomizerTest} verifica la riga JSON finale; qui si verifica il lato
 * <em>scrittura</em>, cioe' che la chiave entri nell'MDC con il prefisso giusto e ne esca pulita.
 * Sono due contratti distinti e si rompono in modi diversi: un prefisso sbagliato qui produce un
 * dettaglio che il customizer non riconosce e che sparisce dal JSON <strong>senza errore</strong>.
 */
class LogDetailsTest {

    @AfterEach
    void cleanUp() {
        MDC.clear();
    }

    private static Map<String, String> mdc() {
        Map<String, String> snapshot = MDC.getCopyOfContextMap();
        return snapshot == null ? Map.of() : snapshot;
    }

    /** Il tipo Java sceglie il prefisso: e' il meccanismo che sostituisce un registro chiave/tipo. */
    @Test
    void ilPrefissoSegueIlTipoJava() {
        try (LogDetails ignored = LogDetails.open()
                .with("un_intero", 7)
                .with("un_long", 7L)
                .with("un_double", 1.5)
                .with("un_booleano", true)
                .with("un_testo", "x")
                .with("un_istante", Instant.EPOCH)
                .with("una_durata", Duration.ofSeconds(3))) {

            Map<String, String> mdc = mdc();
            assertEquals("7", mdc.get(LogFields.DETAIL_NUMERIC_PREFIX + "un_intero"));
            assertEquals("7", mdc.get(LogFields.DETAIL_NUMERIC_PREFIX + "un_long"));
            assertEquals("1.5", mdc.get(LogFields.DETAIL_NUMERIC_PREFIX + "un_double"));
            assertEquals("true", mdc.get(LogFields.DETAIL_BOOLEAN_PREFIX + "un_booleano"));
            assertEquals("x", mdc.get(LogFields.DETAIL_TEXT_PREFIX + "un_testo"));
            // Un Instant non e' un Number: deve finire fra i testuali, non fra i numerici.
            assertTrue(mdc.containsKey(LogFields.DETAIL_TEXT_PREFIX + "un_istante"));
            assertTrue(mdc.containsKey(LogFields.DETAIL_TEXT_PREFIX + "una_durata"));
        }
    }

    /**
     * Un valore nullo non scrive la chiave. Un campo assente deve restare assente: uno zero o la
     * stringa "null" sarebbero una misura falsa su cui qualcuno aggregherebbe.
     */
    @Test
    void unValoreNulloNonScriveLaChiave() {
        try (LogDetails ignored = LogDetails.open().with("assente", null).with("presente", 1)) {
            assertNull(mdc().get(LogFields.DETAIL_TEXT_PREFIX + "assente"));
            assertNull(mdc().get(LogFields.DETAIL_NUMERIC_PREFIX + "assente"));
            assertEquals("1", mdc().get(LogFields.DETAIL_NUMERIC_PREFIX + "presente"));
        }
    }

    /** Un nome nullo non deve far saltare la riga di log: si ignora il dettaglio. */
    @Test
    void unNomeNulloVieneIgnorato() {
        try (LogDetails ignored = LogDetails.open().with(null, "x").with("buono", "y")) {
            assertEquals("y", mdc().get(LogFields.DETAIL_TEXT_PREFIX + "buono"));
        }
    }

    /**
     * I dettagli valgono per una riga sola. I thread dei pool Quartz sono riusati: una chiave
     * rimasta attribuirebbe al job successivo un dato che non e' suo.
     */
    @Test
    void nullaSopravviveAlloScope() {
        try (LogDetails ignored = LogDetails.open().with("effimero", 1)) {
            assertEquals("1", mdc().get(LogFields.DETAIL_NUMERIC_PREFIX + "effimero"));
        }
        assertTrue(mdc().isEmpty(), "dettagli rimasti nell'MDC: " + mdc());
    }

    /** Scope annidati: quello interno ripristina, non cancella. */
    @Test
    void unoScopeInternoRipristinaQuelloEsterno() {
        try (LogDetails esterno = LogDetails.open().with("rows", 1)) {
            try (LogDetails interno = LogDetails.open().with("rows", 2)) {
                assertEquals("2", mdc().get(LogFields.DETAIL_NUMERIC_PREFIX + "rows"));
            }
            assertEquals("1", mdc().get(LogFields.DETAIL_NUMERIC_PREFIX + "rows"),
                    "lo scope interno ha cancellato il dettaglio del chiamante");
        }
    }

    /** {@link MdcScope#run} imposta le tre chiavi che ogni servizio senza RunContext deve avere. */
    @Test
    void runImpostaContestoEfase() {
        try (MdcScope ignored = MdcScope.run("run-1", "POSITION", "END")) {
            assertEquals("run-1", MDC.get(LogFields.CORRELATION_ID));
            assertEquals("POSITION", MDC.get(LogFields.EVENT_ACTION));
            assertEquals("END", MDC.get(LogFields.CTX_PHASE));
        }
        assertNull(MDC.get(LogFields.CORRELATION_ID));
        assertNull(MDC.get(LogFields.EVENT_ACTION));
        assertNull(MDC.get(LogFields.CTX_PHASE));
    }

    /**
     * Un dettaglio non deve poter finire di primo livello travestito da attributo indicizzato.
     *
     * <p>Il nome scritto dal chiamante arriva <strong>sempre</strong> prefissato: se un giorno
     * qualcuno passasse per {@code MDC.put} diretto il campo comparirebbe nel dizionario di primo
     * livello, che il documento vuole chiuso.
     */
    @Test
    void ogniDettaglioEprefissato() {
        try (LogDetails ignored = LogDetails.open().with("dependency", "postgres").with("rows", 3)) {
            for (String key : mdc().keySet()) {
                assertTrue(LogFields.DETAIL_PREFIXES.stream().anyMatch(key::startsWith),
                        "chiave di dettaglio senza prefisso: " + key);
            }
        }
    }
}
