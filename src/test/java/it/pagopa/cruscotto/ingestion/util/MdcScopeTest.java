package it.pagopa.cruscotto.ingestion.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MdcScopeTest {

    @AfterEach
    void cleanUp() {
        MDC.clear();
    }

    /**
     * Il difetto per cui questa classe esiste.
     *
     * <p>Lo scanner della ricerca massiva imposta {@code entityName} e {@code runId}, poi chiama il
     * servizio di esecuzione che imposta le proprie chiavi e, uscendo, faceva {@code MDC.remove}
     * anche di quelle del chiamante: da li' in poi i log dello scanner restavano senza contesto.
     * Con l'encoder ECS quelle chiavi sono attributi di primo livello del JSON, cioe' i campi su
     * cui si filtra, quindi il difetto e' passato da invisibile a rilevante.</p>
     */
    @Test
    void loScopeInternoRipristinaIlContestoDelChiamanteInveceDiCancellarlo() {
        try (MdcScope esterno = MdcScope.open().with("entityName", "SCANNER").with("runId", "run-1")) {
            try (MdcScope interno = MdcScope.open().with("entityName", "MASSIVE_SEARCH").with("runId", "exec-9")) {
                assertEquals("MASSIVE_SEARCH", MDC.get("entityName"));
                assertEquals("exec-9", MDC.get("runId"));
            }

            assertEquals("SCANNER", MDC.get("entityName"), "lo scope interno ha cancellato il contesto esterno");
            assertEquals("run-1", MDC.get("runId"), "lo scope interno ha cancellato il contesto esterno");
        }
    }

    /**
     * La pulizia resta obbligatoria: i thread dei pool Quartz sono riusati, e una chiave rimasta
     * attribuirebbe al job successivo un contesto che non e' suo — peggio che non averne.
     */
    @Test
    void allUscitaDelloScopePiuEsternoNonRestaNulla() {
        try (MdcScope ignored = MdcScope.open().with("entityName", "POSITION").with("runId", "run-1")) {
            assertEquals("POSITION", MDC.get("entityName"));
        }

        assertNull(MDC.get("entityName"));
        assertNull(MDC.get("runId"));
    }

    /** Lo scope si estende durante l'esecuzione: il servizio aggiunge executionId a meta' metodo. */
    @Test
    void unaChiaveAggiuntaDopoVieneComunqueRipulita() {
        try (MdcScope scope = MdcScope.open().with("instanceId", "i-1")) {
            scope.with("executionId", "e-1");
            assertEquals("e-1", MDC.get("executionId"));
        }

        assertNull(MDC.get("executionId"));
        assertNull(MDC.get("instanceId"));
    }

    /**
     * Con due {@code with()} sulla stessa chiave il valore da ripristinare deve restare quello di
     * partenza, non quello intermedio: altrimenti uscendo si lascerebbe un valore mai esistito.
     */
    @Test
    void ripristinaIlValoreInizialeNonQuelloIntermedio() {
        MDC.put("entityName", "ORIGINALE");

        try (MdcScope scope = MdcScope.open().with("entityName", "PRIMO")) {
            scope.with("entityName", "SECONDO");
            assertEquals("SECONDO", MDC.get("entityName"));
        }

        assertEquals("ORIGINALE", MDC.get("entityName"));
    }

    /** Un valore nullo toglie la chiave per la durata dello scope invece di lasciare quella altrui. */
    @Test
    void unValoreNulloNonEreditaIlValoreDiUnContestoEstraneo() {
        MDC.put("runId", "di-qualcun-altro");

        try (MdcScope ignored = MdcScope.open().with("runId", null)) {
            assertNull(MDC.get("runId"));
        }

        assertEquals("di-qualcun-altro", MDC.get("runId"));
    }
}
