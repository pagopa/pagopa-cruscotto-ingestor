package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.service.AnagraficaService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * PA_EMITTENTE (ID_DOMINIO) is registered in ANAG_PA_EMITTENTE only when it is a valid 11-digit
 * numeric fiscal code. Dirty source values must not pollute the frontend filter registry. POSITION
 * keeps the raw string regardless (no FK to the registry), so this filter never drops a transaction.
 */
@ExtendWith(MockitoExtension.class)
class EntityTransformerImplPaEmittenteFilterTest {

    @Mock
    private AnagraficaService anagraficaService;

    private EntityTransformerImpl transformer() {
        // resolveAllAnagrafiche only collaborates with AnagraficaService.
        return new EntityTransformerImpl(null, anagraficaService, null, null, null);
    }

    private void resolve(String paEmittente) {
        Map<String, Object> row = new HashMap<>();
        row.put("PA_EMITTENTE", paEmittente);
        transformer().resolveAllAnagrafiche("run-test", row);
    }

    @Test
    void registersValidElevenDigitCode() {
        resolve("12345678901");
        verify(anagraficaService).resolvePaEmittenteId("run-test", "12345678901");
    }

    @Test
    void trimsBeforeRegistering() {
        resolve("  12345678901  ");
        verify(anagraficaService).resolvePaEmittenteId("run-test", "12345678901");
    }

    @Test
    void skipsCodeShorterThanEleven() {
        resolve("1234567890");
        verify(anagraficaService, never()).resolvePaEmittenteId(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void skipsCodeLongerThanEleven() {
        resolve("123456789012");
        verify(anagraficaService, never()).resolvePaEmittenteId(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void skipsNonNumericCode() {
        resolve("1234567890X");
        verify(anagraficaService, never()).resolvePaEmittenteId(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString());
    }

    @Test
    void skipsGarbageLongerThanColumnWidth() {
        // The clamp-to-255 side effect: oversized junk previously became a distinct registry row.
        resolve("A".repeat(255));
        verify(anagraficaService, never()).resolvePaEmittenteId(org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString());
    }
}
