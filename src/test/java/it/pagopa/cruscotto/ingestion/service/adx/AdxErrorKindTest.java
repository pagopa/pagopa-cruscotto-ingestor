package it.pagopa.cruscotto.ingestion.service.adx;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ancorato ai messaggi <strong>realmente osservati in produzione</strong> durante l'incidente ADX
 * del 6-7 ottobre 2026, copiati verbatim dal log di esecuzione.
 *
 * <p>Il valore del test sta tutto qui: i pattern scritti "a intuito" non intercettano le stringhe
 * del vendor. Nella configurazione esistente {@code "TooManyRequests"} non riconosce
 * {@code "too many requests"} (parole separate) e {@code "PartialQueryFailure"} non riconosce
 * {@code "Partial query failure"} (spazi). Un test che usa messaggi inventati li avrebbe dichiarati
 * corretti entrambi.</p>
 */
class AdxErrorKindTest {

    /** Verbatim dal log, 07/10/2026 11:10. */
    private static final String THROTTLE_REALE =
        "AdxQueryFailedException: ADX query failed: runId=9e88b1a5, entityName=POSITION_TRANSFERS,"
            + " cursor=2026-10-07T05:59:59.932148Z, window=PT5M,"
            + " adxError=ThrottleException: Request was throttled, too many requests.";

    /** Verbatim dal log, 07/10/2026 12:10. Contiene TRE marcatori diversi. */
    private static final String ROWSTORE_REALE =
        "AdxQueryFailedException: ADX query failed: runId=20c442aa, entityName=POSITION_TRANSFERS,"
            + " adxError=DataServiceException: Error found while parsing json response"
            + " | inner=DataWebException: {\"error\":{\"code\":\"Internal service error\","
            + "\"message\":\"Request aborted due to an internal service error.\","
            + "\"@message\":\"Query execution has resulted in error (0x80DA0069):"
            + " Partial query failure: Failed to retrieve values from Rowstore"
            + " (E_RS_CANNOT_RETRIEVE_VALUES_ERROR). (message: Cannot retrieve requested values range: ).\","
            + "\"@failureCode\":520,\"@permanent\":false}}";

    @Test
    void riconosceIlThrottlingRealeDelVendor() {
        assertEquals(AdxErrorKind.THROTTLED, AdxErrorKind.classify(THROTTLE_REALE));
    }

    /**
     * Il caso che giustifica l'ordinamento esplicito delle regole.
     *
     * <p>Il messaggio del rowstore contiene anche {@code "Partial query failure"} e
     * {@code "Internal service error"}: senza una precedenza, finirebbe in una categoria generica e
     * dieci fallimenti identici risulterebbero indistinguibili dal rumore di fondo.</p>
     */
    @Test
    void ilRowstoreVinceSuiMarcatoriGenericiCheIlMessaggioContieneComunque() {
        assertEquals(AdxErrorKind.ROWSTORE, AdxErrorKind.classify(ROWSTORE_REALE));
    }

    @Test
    void leDueFamigliieDellIncidenteRestanoDistinguibili() {
        assertEquals(AdxErrorKind.THROTTLED, AdxErrorKind.classify(THROTTLE_REALE));
        assertEquals(AdxErrorKind.ROWSTORE, AdxErrorKind.classify(ROWSTORE_REALE));
    }

    @Test
    void riconosceLeAltreCategorieNote() {
        assertEquals(AdxErrorKind.RESULT_TOO_LARGE, AdxErrorKind.classify(
            "LimitsExceeded: exceeds the set limit of 64 MB (E_QUERY_RESULT_SET_TOO_LARGE)"));
        assertEquals(AdxErrorKind.TIMEOUT, AdxErrorKind.classify(
            "java.net.SocketTimeoutException: Read timed out"));
        assertEquals(AdxErrorKind.NETWORK, AdxErrorKind.classify("Connection reset"));
        assertEquals(AdxErrorKind.LOW_MEMORY, AdxErrorKind.classify(
            "E_LOW_MEMORY_CONDITION: query exceeded its memory budget"));
    }

    @Test
    void unErroreNonRiconosciutoNonInventaUnaCategoria() {
        assertEquals(AdxErrorKind.UNKNOWN, AdxErrorKind.classify("Semantic error: 'x' could not be resolved"));
        assertEquals(AdxErrorKind.UNKNOWN, AdxErrorKind.classify(null));
        assertEquals(AdxErrorKind.UNKNOWN, AdxErrorKind.classify("   "));
    }

    /**
     * Il nome dell'eccezione resta il prefisso: ricerche e alert gia' impostati su
     * {@code AdxQueryFailedException} continuano a funzionare, e la categoria si aggiunge.
     */
    @Test
    void ilCodiceMantieneIlNomeDellEccezioneEAggiungeLaCategoria() {
        assertEquals("AdxQueryFailedException/THROTTLED",
            AdxErrorKind.errorCode("AdxQueryFailedException", THROTTLE_REALE));
        assertEquals("AdxQueryFailedException/ROWSTORE",
            AdxErrorKind.errorCode("AdxQueryFailedException", ROWSTORE_REALE));
    }

    /** Senza categoria riconosciuta il codice resta identico a prima: nessuna regressione. */
    @Test
    void unErroreNonClassificabileLasciaIlCodiceInvariato() {
        assertEquals("IllegalStateException",
            AdxErrorKind.errorCode("IllegalStateException", "fetchWindow returned no result"));
    }

    // ─── Comportamento: quale rimedio si applica a quale categoria ────────────────────────────

    /**
     * Il throttling si cura con il backoff, <strong>mai</strong> restringendo la finestra.
     *
     * <p>E' il difetto trovato nell'analisi dell'incidente ADX del 6-7 ottobre 2026: la stringa
     * {@code "Aborted due to throttling"} stava fra i casi curabili dimezzando la finestra, quindi
     * un rifiuto all'ammissione veniva escluso dal retry con backoff e curato restringendo e
     * rilanciando <em>senza pausa</em>, fino a dieci volte. Era l'unica via di amplificazione
     * trovata in tutta l'analisi: il cluster chiedeva di rallentare e noi acceleravamo.
     *
     * <p>Un rifiuto all'ammissione non dipende da quanto si e' chiesto: restringere non serve.
     */
    @Test
    void ilThrottlingSiCuraConIlBackoffNonRestringendoLaFinestra() {
        AdxErrorKind kind = AdxErrorKind.classify(THROTTLE_REALE);

        assertEquals(AdxErrorKind.THROTTLED, kind);
        assertTrue(kind.worthRetrying(), "il throttling va ritentato con backoff");
        assertFalse(kind.curableByNarrowingWindow(),
            "restringere la finestra su un throttling aggiunge pressione invece di toglierne");
    }

    /** Il contrario: un limite sulla dimensione del risultato si cura restringendo, non ritentando. */
    @Test
    void ilRisultatoTroppoGrandeSiCuraRestringendoNonRitentando() {
        AdxErrorKind kind = AdxErrorKind.classify(
            "LimitsExceeded: Query result set has exceeded the internal record count limit");

        assertEquals(AdxErrorKind.RESULT_TOO_LARGE, kind);
        assertTrue(kind.curableByNarrowingWindow());
        assertFalse(kind.worthRetrying(), "ritentare identica una query troppo grande la rifa' fallire");
    }

    /**
     * I due rimedi sono <strong>disgiunti</strong>: nessuna categoria puo' essere curata in
     * entrambi i modi, e nessuna puo' restare senza rimedio per errore di classificazione.
     *
     * <p>E' l'invariante che il difetto violava: con le due liste separate in configurazione, e
     * confrontate con due criteri diversi, un errore poteva essere escluso dal retry
     * <em>senza</em> diventare eleggibile al dimezzamento, e cadere in un buco.
     */
    @Test
    void iDueRimediSonoDisgiunti() {
        for (AdxErrorKind kind : AdxErrorKind.values()) {
            assertFalse(kind.worthRetrying() && kind.curableByNarrowingWindow(),
                kind + " e' classificata come curabile in due modi incompatibili");
        }
    }

    /** Il rowstore resta fuori dal retry: ogni tentativo durava fra 355 e 463 secondi. */
    @Test
    void ilRowstoreNonVieneRitentato() {
        AdxErrorKind kind = AdxErrorKind.classify(ROWSTORE_REALE);

        assertEquals(AdxErrorKind.ROWSTORE, kind);
        assertFalse(kind.worthRetrying(),
            "tre tentativi da ~400s immobilizzerebbero il job per venti minuti senza evidenza "
                + "che il secondo vada meglio del primo");
        assertFalse(kind.curableByNarrowingWindow());
    }
}
