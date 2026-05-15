package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
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
    private final PositionTokensRepository positionTokensRepository;

    /**
     * Trasformare e applicare regole evento per POSITION_TOKENS.
     * Regola 7.2: Risolvere FK_POSITION via NAV + PA_EMITTENTE.
     * Regola 7.3: Se TOKEN già esiste, impostare ID per UPDATE (segnalare al BulkWriter).
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

            // DATE_EVENT
            Instant insertedTs = toInstant(transformed.get("INSERTED_TIMESTAMP"));
            if (insertedTs != null) {
                token.setDateEvent(insertedTs.atZone(ZoneOffset.UTC).toLocalDate());
            }

            // Implementare Regola 7.2: Associare TOKEN a POSITION via NAV + PA_EMITTENTE (finestra 24h)
            String nav = (String) transformed.get("NAV");
            String paEmittente = (String) transformed.get("PA_EMITTENTE");

            if (nav != null && paEmittente != null && insertedTs != null) {
                LocalDateTime tokenInsertedLdt = toLocalDateTime(insertedTs);
                // Cercare POSITION con (NAV + PA_EMITTENTE) entro 24h prima del token.inserted_timestamp
                LocalDateTime window24hBefore = tokenInsertedLdt.minusHours(24);

                Optional<Integer> fkPositionOpt = positionRepository
                        .findFirstByNavAndPaEmittenteAndInsertedTimestampLessThanEqualOrderByInsertedTimestampDescIdDesc(
                                nav, paEmittente, tokenInsertedLdt)
                        .filter(p -> {
                            long secondsDiff = ChronoUnit.SECONDS.between(p.getInsertedTimestamp(), tokenInsertedLdt);
                            return secondsDiff <= 86400; // 24 hours
                        })
                        .map(p -> p.getId());

                if (fkPositionOpt.isPresent()) {
                    token.setFkPosition(fkPositionOpt.get());
                    log.debug("[{}] [TRANSFORM] POSITION_TOKENS FK_POSITION resolved: fkPosition={} nav={} paEmittente={}",
                            runId, fkPositionOpt.get(), nav, paEmittente);
                } else {
                    log.warn("[{}] [TRANSFORM] POSITION_TOKENS FK_POSITION NOT FOUND: nav={} paEmittente={} insertedTs={}",
                            runId, nav, paEmittente, insertedTs);
                }
            }

            // Applicare regole evento
            applyEventRules(runId, token, transformed);

            // Implementare Regola 7.3: Verificare se TOKEN già esiste
            if (tokenBytes != null && insertedTs != null) {
                Optional<PositionTokens> existingTokenOpt = positionTokensRepository.findLatestByToken(tokenBytes);
                if (existingTokenOpt.isPresent()) {
                    PositionTokens existingToken = existingTokenOpt.get();
                    token.setId(existingToken.getId()); // Segnalare UPDATE nel BulkWriter
                    log.debug("[{}] [TRANSFORM] POSITION_TOKENS UPDATE: id={} token={}",
                            runId, existingToken.getId(),
                            tokenBytes.length > 0 ? "***" : "empty");
                } else {
                    log.debug("[{}] [TRANSFORM] POSITION_TOKENS INSERT: new token", runId);
                }
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
                // Aggiornare OUTCOME con OUTCOME_REQ
                token.setOutcome(outcomeReq);
                // Se OUTCOME_REQ = OK e PAYMENT_DATE vuoto: PAYMENT_DATE = INSERTED_TIMESTAMP_REQ
                if ("OK".equals(outcomeReq) && token.getPaymentDate() == null && insertedTsReq != null) {
                    token.setPaymentDate(insertedTsReq);
                    // Aggiornare PAYMENT_METHOD se Touchpoint = 'Touchpoint PSP'
                    if ("Touchpoint PSP".equals(touchpoint)) {
                        token.setPaymentMethod((String) row.get("PAYMENT_METHOD"));
                    }
                }
            } else if ("KO".equals(outcomeResp)) {
                // Se FAULT_CODE ∈ scaduto e Touchpoint PSP
                if ((isTokenScaduto(faultCode)) && "Touchpoint PSP".equals(touchpoint)) {
                    token.setOutcome(outcomeReq);
                }
                // Se OUTCOME_REQ = OK e PAYMENT_DATE vuoto
                if ("OK".equals(outcomeReq) && token.getPaymentDate() == null && insertedTsReq != null) {
                    token.setPaymentDate(insertedTsReq);
                    if ("Touchpoint PSP".equals(touchpoint)) {
                        token.setPaymentMethod((String) row.get("PAYMENT_METHOD"));
                    }
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

