package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

/**
 * Trasformer specializzato per POSITION_TOKENS.
 * Applica regole evento: sendPaymentOutcome, activatePaymentNotice, pspNotifyPayment, closePayment.
 * Regola 7.2: Associare TOKEN a POSITION via NAV + PA_EMITTENTE entro finestra 24h.
 * Regola 7.3: Se TOKEN già esiste, UPDATE; altrimenti INSERT.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PositionTokensTransformer {

    private final EntityTransformerImpl baseTransformer;
    private final PositionRepository positionRepository;

    /**
     * Trasformare e applicare regole evento per POSITION_TOKENS.
     * Regola 7.2: Risolvere FK_POSITION via NAV + PA_EMITTENTE.
     * Regola 7.3: first-write-wins, nessun update su TOKEN esistente.
     */
    public PositionTokens transform(Map<String, Object> row, RunContext ctx) throws EntityTransformer.TransformationException {
        String runId = ctx.getRunId();

        try {
            Map<String, Object> transformed = new java.util.HashMap<>(row);
            baseTransformer.resolveAllAnagrafiche(runId, transformed);

            PositionTokens token = new PositionTokens();

            // Campi base
            byte[] tokenBytes = toBytes(transformed.get("TOKEN"));
            token.setToken(tokenBytes);
            token.setIuv((String) transformed.get("IUV"));

            // CREDITOR_REF_ID: valorizzare SOLO se diverso da IUV
            String creditorRefId = (String) transformed.get("CREDITOR_REF_ID");
            String iuv = token.getIuv();
            if (creditorRefId != null && !creditorRefId.equals(iuv)) {
                token.setCreditorRefId(creditorRefId);
            }

            // Cast numerici
            token.setAmount(toBigDecimal(transformed.get("AMOUNT")));
            token.setFee(toBigDecimal(transformed.get("FEE")));

            // Anagrafiche (già risolte in IDs)
            token.setStazione((Short) transformed.get("STAZIONE"));
            token.setCanale((Short) transformed.get("CANALE"));
            token.setPsp((Short) transformed.get("PSP"));
            token.setIntermediarioPa((Short) transformed.get("INTERMEDIARIO_PA"));
            token.setIntermediarioPsp((Short) transformed.get("INTERMEDIARIO_PSP"));

            // Touchpoint
            token.setTouchpoint((String) transformed.get("TOUCHPOINT"));

            // DATE_EVENT + timestamp sorgente ADX. Entrambe derivate dallo stesso istante UTC e
            // scritte solo all'insert (registry-gated, first-write-wins): invariante
            // DATE_EVENT = date(INSERTED_TIMESTAMP), su cui si appoggia il pruning dei report.
            Instant insertedTs = toInstant(transformed.get("INSERTED_TIMESTAMP"));
            if (insertedTs != null) {
                token.setDateEvent(insertedTs.atZone(ZoneOffset.UTC).toLocalDate());
                token.setInsertedTimestamp(toLocalDateTime(insertedTs));
            }

            // Implementare Regola 7.2: Associare TOKEN a POSITION via NAV + PA_EMITTENTE (finestra 24h)
            String nav = (String) transformed.get("NAV");
            String paEmittente = (String) transformed.get("PA_EMITTENTE");

            if (nav != null && paEmittente != null && insertedTs != null) {
                LocalDateTime tokenInsertedLdt = toLocalDateTime(insertedTs);
                // Cercare POSITION con (NAV + PA_EMITTENTE) entro 24h prima del token.inserted_timestamp.
                // Lookup con partition pruning: il bound 24h (ex filtro secondsDiff) e' ora nel SQL.
                Optional<Integer> fkPositionOpt = positionRepository
                        .findLatestByBusinessKeyWithin24h(nav, paEmittente, tokenInsertedLdt)
                        .map(p -> p.getId());

                if (fkPositionOpt.isPresent()) {
                    Integer fkPosition = fkPositionOpt.orElseThrow(
                            () -> new IllegalStateException("FK_POSITION unexpectedly absent for nav=" + nav));
                    token.setFkPosition(fkPosition);
                    log.debug("[{}] [TRANSFORM] POSITION_TOKENS FK_POSITION resolved: fkPosition={} nav={} paEmittente={}",
                            runId, fkPosition, nav, paEmittente);
                } else {
                    log.warn("[{}] [TRANSFORM] POSITION_TOKENS FK_POSITION NOT FOUND: nav={} paEmittente={} insertedTs={}",
                            runId, nav, paEmittente, insertedTs);
                }
            }

            // Applicare regole evento
            applyEventRules(runId, token, transformed);

            if (tokenBytes != null && insertedTs != null) {
                log.debug("[{}] [TRANSFORM] POSITION_TOKENS INSERT attempt for token={}",
                        runId, tokenBytes.length > 0 ? "***" : "empty");
            }

            return token;
        } catch (Exception e) {
            log.error("[{}] [TRANSFORM] Failed to transform POSITION_TOKENS: {}", runId, e.getMessage(), e);
            throw new EntityTransformer.TransformationException("POSITION_TOKENS transform failed", e);
        }
    }

    /**
     * Applicare regole basate sul tipo di evento.
     */
    private void applyEventRules(String runId, PositionTokens token, Map<String, Object> row) {
        String tipoEvento = (String) row.get("TIPO_EVENTO");
        String sottoTipoEvento = (String) row.get("SOTTO_TIPO_EVENTO");
        String outcomeReq = (String) row.get("OUTCOME_REQ");
        String outcomeResp = (String) row.get("OUTCOME_RESP");
        String faultCode = (String) row.get("FAULT_CODE");
        String touchpoint = token.getTouchpoint();
        LocalDateTime insertedTsReq = toLocalDateTime(toInstant(row.get("INSERTED_TIMESTAMP_REQ")));

        // sendPaymentOutcome / V2
        if ("sendPaymentOutcome".equals(tipoEvento)) {
            if ("OK".equals(outcomeResp)) {
                // Aggiornare OUTCOME con OUTCOME_REQ, se l'evento lo porta: il requisito dice "con il
                // valore di OUTCOME_REQ presente nell'evento", quindi un evento senza esito non e'
                // titolato a cancellare un esito gia' registrato.
                setOutcomeIfPresent(token, outcomeReq);
                // Punti 2 e 3 del requisito, esplicitamente NON in AND fra loro: PAYMENT_DATE dipende da
                // OUTCOME_REQ = OK, PAYMENT_METHOD solo dal touchpoint. Annidare il secondo nel primo lo
                // saltava sia con OUTCOME_REQ = KO sia quando PAYMENT_DATE era gia' stata scritta da una
                // SPO precedente.
                if ("OK".equals(outcomeReq) && token.getPaymentDate() == null && insertedTsReq != null) {
                    token.setPaymentDate(insertedTsReq);
                }
                if ("Touchpoint PSP".equals(touchpoint)) {
                    setPaymentMethodIfAbsent(token, (String) row.get("PAYMENT_METHOD"));
                }
            } else if ("KO".equals(outcomeResp)) {
                // Guard del Caso 2 del requisito: FAULT_CODE di token scaduto E Touchpoint PSP. Il
                // popolamento di PAYMENT_DATE sta DENTRO il guard, non accanto: tenerlo fuori valorizzava
                // la data di pagamento per qualunque SPO KO con OUTCOME_REQ = OK (altro FAULT_CODE, altro
                // touchpoint) senza mai toccare OUTCOME, producendo token con PAYMENT_DATE valorizzata e
                // OUTCOME nullo. Oltre a essere una falsa data di incasso, rompeva l'equivalenza
                // PAYMENT_DATE IS NOT NULL <=> OUTCOME = 'OK' su cui si appoggiano IS_PAYED e DATE_PAYED
                // nei report della ricerca massiva.
                if (isTokenScaduto(faultCode) && "Touchpoint PSP".equals(touchpoint)) {
                    setOutcomeIfPresent(token, outcomeReq);
                    if ("OK".equals(outcomeReq) && token.getPaymentDate() == null && insertedTsReq != null) {
                        token.setPaymentDate(insertedTsReq);
                    }
                    // Punto 3 del requisito, esplicitamente NON in AND con il punto 2: come nel ramo
                    // OUTCOME_RESP = OK, PAYMENT_METHOD e' indipendente da PAYMENT_DATE. Touchpoint PSP
                    // e' gia' garantito dal guard esterno, quindi qui non va ripetuto.
                    setPaymentMethodIfAbsent(token, (String) row.get("PAYMENT_METHOD"));
                }
            }
        }

        // activatePaymentNotice / V2
        if ("activatePaymentNotice".equals(tipoEvento)) {
            if ("OK".equals(outcomeResp)) {
                // Valorizzare CREDITOR_REF_ID SOLO se diverso da IUV
                String creditorRefId = (String) row.get("CREDITOR_REF_ID");
                if (creditorRefId != null && !creditorRefId.equals(token.getIuv())) {
                    token.setCreditorRefId(creditorRefId);
                }
            }
        }

        // pspNotifyPayment / V2
        if ("pspNotifyPayment".equals(tipoEvento)) {
            if ("OK".equals(outcomeResp)) {
                // Aggiornare PSP, INTERMEDIARIO_PSP, CANALE
                Short psp = (Short) row.get("PSP");
                if (psp != null) token.setPsp(psp);
                Short intermediarioPsp = (Short) row.get("INTERMEDIARIO_PSP");
                if (intermediarioPsp != null) token.setIntermediarioPsp(intermediarioPsp);
                Short canale = (Short) row.get("CANALE");
                if (canale != null) token.setCanale(canale);
            } else if ("KO".equals(outcomeResp)) {
                // Se OUTCOME del TOKEN è vuoto: OUTCOME = KO
                if (token.getOutcome() == null || token.getOutcome().isBlank()) {
                    token.setOutcome("KO");
                }
            }
        }

        // closePayment / V2
        if ("closePayment".equals(tipoEvento)) {
            if ("OK".equals(outcomeReq) && "OK".equals(outcomeResp)) {
                // Aggiornare PAYMENT_METHOD, PSP, INTERMEDIARIO_PSP, CANALE
                token.setPaymentMethod((String) row.get("PAYMENT_METHOD"));
                Short psp = (Short) row.get("PSP");
                if (psp != null) token.setPsp(psp);
                Short intermediarioPsp = (Short) row.get("INTERMEDIARIO_PSP");
                if (intermediarioPsp != null) token.setIntermediarioPsp(intermediarioPsp);
                Short canale = (Short) row.get("CANALE");
                if (canale != null) token.setCanale(canale);
            } else if ("KO".equals(outcomeReq) && "OK".equals(outcomeResp)) {
                // Se OUTCOME TOKEN vuoto: OUTCOME = KO
                if (token.getOutcome() == null || token.getOutcome().isBlank()) {
                    token.setOutcome("KO");
                }
            }
        }

        log.debug("[{}] [TRANSFORM] POSITION_TOKENS event rules applied: tipoEvento={} outcome={}",
                runId, tipoEvento, token.getOutcome());
    }

    private boolean isTokenScaduto(String faultCode) {
        return "PPT_TOKEN_SCADUTO".equals(faultCode) || "PPT_TOKEN_SCADUTO_KO".equals(faultCode);
    }

    /**
     * Scrive PAYMENT_METHOD solo se il token non lo ha gia' valorizzato ("Valorizzare solo se e' null"
     * del requisito SPO): il primo evento che lo porta vince, i successivi non lo riallineano. Un
     * valore nullo in arrivo non azzera quello esistente.
     */
    private void setPaymentMethodIfAbsent(PositionTokens token, String paymentMethod) {
        if (paymentMethod != null && token.getPaymentMethod() == null) {
            token.setPaymentMethod(paymentMethod);
        }
    }

    /**
     * Scrive OUTCOME solo se l'evento ne porta uno: il requisito parla del "valore di OUTCOME_REQ
     * presente nell'evento", quindi un evento che non lo valorizza non deve cancellare l'esito gia'
     * registrato. Senza questo guard un SPO senza OUTCOME_REQ azzerava un 'OK' precedente lasciando
     * PAYMENT_DATE valorizzata, cioe' un token con data di incasso ma non incassato.
     */
    private boolean setOutcomeIfPresent(PositionTokens token, String outcomeReq) {
        if (outcomeReq == null || outcomeReq.isBlank() || outcomeReq.equals(token.getOutcome())) {
            return false;
        }
        token.setOutcome(outcomeReq);
        return true;
    }

    private byte[] toBytes(Object value) {
        if (value == null) return null;
        if (value instanceof byte[] bytes) return bytes;
        if (value instanceof String str) return str.getBytes();
        return null;
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof BigDecimal bd) return bd;
            if (value instanceof Number num) return BigDecimal.valueOf(num.doubleValue());
            if (value instanceof String str) return new BigDecimal(str);
        } catch (Exception e) {
            log.warn("Failed to convert to BigDecimal: {}", value);
        }
        return null;
    }

    private Instant toInstant(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof Instant inst) return inst;
            if (value instanceof java.time.LocalDateTime ldt) return ldt.toInstant(ZoneOffset.UTC);
            if (value instanceof Long epoch) return Instant.ofEpochMilli((Long) value);
        } catch (Exception ignored) {}
        return null;
    }

    private LocalDateTime toLocalDateTime(Instant inst) {
        if (inst == null) return null;
        return java.time.LocalDateTime.ofInstant(inst, ZoneOffset.UTC);
    }
}
