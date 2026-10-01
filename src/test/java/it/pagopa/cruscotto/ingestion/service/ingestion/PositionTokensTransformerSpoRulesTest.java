package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Regole sendPaymentOutcome/V2 su POSITION_TOKENS.
 * <p>
 * L'invariante da proteggere e' PAYMENT_DATE IS NOT NULL &lt;=&gt; OUTCOME = 'OK': i report della
 * ricerca massiva espongono IS_PAYED e DATE_PAYED assumendola, quindi una PAYMENT_DATE scritta
 * senza il corrispondente OUTCOME si traduce in una falsa data di incasso nel CSV.
 */
@ExtendWith(MockitoExtension.class)
class PositionTokensTransformerSpoRulesTest {

    private static final Instant TS = Instant.parse("2026-09-25T10:15:30Z");

    @Mock
    private EntityTransformerImpl baseTransformer;

    @Mock
    private PositionRepository positionRepository;

    private PositionTokensTransformer transformer;

    @BeforeEach
    void setUp() {
        transformer = new PositionTokensTransformer(baseTransformer, positionRepository);
    }

    @Test
    void outcomeOkPopulatesBothOutcomeAndPaymentDate() throws Exception {
        PositionTokens token = transform(spo("OK", "OK", null, "Touchpoint PSP"));

        assertEquals("OK", token.getOutcome());
        assertEquals(LocalDateTime.ofInstant(TS, java.time.ZoneOffset.UTC), token.getPaymentDate());
        assertEquals("CP", token.getPaymentMethod());
    }

    @Test
    void outcomeRespKoOnExpiredTokenFromPspTouchpointPopulatesBoth() throws Exception {
        // Unico ramo KO ammesso dal requisito: FAULT_CODE di token scaduto AND Touchpoint PSP AND
        // OUTCOME_REQ = OK, tutte in AND.
        PositionTokens token = transform(spo("OK", "KO", "PPT_TOKEN_SCADUTO", "Touchpoint PSP"));

        assertEquals("OK", token.getOutcome());
        assertNotNull(token.getPaymentDate());
        assertEquals("CP", token.getPaymentMethod());
    }

    @Test
    void outcomeRespKoWithUnrelatedFaultCodeNeverWritesPaymentDate() throws Exception {
        // Regressione: il popolamento di PAYMENT_DATE stava fuori dal guard, quindi scattava per
        // qualunque SPO KO con OUTCOME_REQ = OK senza mai valorizzare OUTCOME -> token con data di
        // pagamento e OUTCOME nullo (invisibile anche alle verifiche "outcome <> 'OK'", che in SQL
        // non matchano i NULL).
        PositionTokens token = transform(spo("OK", "KO", "PPT_STAZIONE_INT_PA_IRRAGGIUNGIBILE", "Touchpoint PSP"));

        assertNull(token.getOutcome());
        assertNull(token.getPaymentDate());
    }

    @Test
    void outcomeRespKoOnExpiredTokenOutsidePspTouchpointNeverWritesPaymentDate() throws Exception {
        PositionTokens token = transform(spo("OK", "KO", "PPT_TOKEN_SCADUTO", "Touchpoint Checkout"));

        assertNull(token.getOutcome());
        assertNull(token.getPaymentDate());
    }

    @Test
    void paymentMethodIsUpdatedIndependentlyOfPaymentDate() throws Exception {
        // Requisito: con OUTCOME_RESP = OK i due casi sono indipendenti. Con OUTCOME_REQ = KO non c'e'
        // incasso (nessuna PAYMENT_DATE) ma il metodo di pagamento va comunque riallineato.
        PositionTokens token = transform(spo("KO", "OK", null, "Touchpoint PSP"));

        assertEquals("KO", token.getOutcome());
        assertNull(token.getPaymentDate());
        assertEquals("CP", token.getPaymentMethod());
    }

    @Test
    void paymentMethodIsNotTouchedOutsidePspTouchpoint() throws Exception {
        PositionTokens token = transform(spo("OK", "OK", null, "Touchpoint Checkout"));

        assertEquals("OK", token.getOutcome());
        assertNotNull(token.getPaymentDate());
        assertNull(token.getPaymentMethod());
    }

    @Test
    void expiredTokenWithOutcomeReqKoSetsOutcomeAndPaymentMethodButNoPaymentDate() throws Exception {
        // Requisito (Caso 2, punto 3 "NON IN AND con il punto 2"): anche nel ramo KO PAYMENT_METHOD e'
        // indipendente da PAYMENT_DATE. Senza OUTCOME_REQ = OK non c'e' incasso e la data resta nulla,
        // ma il metodo di pagamento va comunque valorizzato. Touchpoint PSP e' gia' nel guard.
        PositionTokens token = transform(spo("KO", "KO", "PPT_TOKEN_SCADUTO", "Touchpoint PSP"));

        assertEquals("KO", token.getOutcome());
        assertNull(token.getPaymentDate());
        assertEquals("CP", token.getPaymentMethod());
    }

    @Test
    void nullPaymentMethodFromEventDoesNotWriteTheToken() throws Exception {
        Map<String, Object> row = spo("OK", "OK", null, "Touchpoint PSP");
        row.put("PAYMENT_METHOD", null);

        PositionTokens token = transform(row);

        assertEquals("OK", token.getOutcome());
        assertNull(token.getPaymentMethod());
    }

    private PositionTokens transform(Map<String, Object> row) throws Exception {
        RunContext ctx = new RunContext("run-test", "POSITION_TOKENS", TS);
        return transformer.transform(row, ctx);
    }

    private static Map<String, Object> spo(String outcomeReq, String outcomeResp, String faultCode, String touchpoint) {
        Map<String, Object> row = new HashMap<>();
        row.put("TIPO_EVENTO", "sendPaymentOutcome");
        row.put("OUTCOME_REQ", outcomeReq);
        row.put("OUTCOME_RESP", outcomeResp);
        row.put("FAULT_CODE", faultCode);
        row.put("TOUCHPOINT", touchpoint);
        row.put("PAYMENT_METHOD", "CP");
        row.put("INSERTED_TIMESTAMP", TS);
        row.put("INSERTED_TIMESTAMP_REQ", TS);
        row.put("TOKEN", "token-abc");
        row.put("IUV", "IUV-1");
        return row;
    }
}
