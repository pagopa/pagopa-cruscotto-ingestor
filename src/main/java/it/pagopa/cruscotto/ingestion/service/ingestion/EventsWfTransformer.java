package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

/**
 * Trasformer specializzato per EVENTS_WF.
 * Regola 7.5:
 * - Se token presente: associare via TOKEN (FK_TOKENS).
 * - Se token assente: associare via (NAV + PA_EMITTENTE) (FK_POSITION).
 * - In caso di multi-payment (IS_EVENT_MULTI_PAYMENT=true), preferire TOKEN.
 * - Aggiornare POSITION.LAST_EVENT e POSITION.DATE_EVENTS dopo l'inserimento.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventsWfTransformer {

    private final EntityTransformerImpl baseTransformer;
    private final PositionRepository positionRepository;
    private final PositionTokensRepository positionTokensRepository;

    /**
     * Trasformare un EVENTS_WF.
     * Regola 7.5: Risolvere FK_TOKENS (preferito) o FK_POSITION.
     * - Se TOKEN presente: cercare TOKEN via chiave TOKEN.
     * - Se TOKEN assente: cercare POSITION via (NAV + PA_EMITTENTE) e finestra 24h.
     * - Se IS_EVENT_MULTI_PAYMENT=true, usare TOKEN per disambiguare.
     */
    public EventsWf transform(Map<String, Object> row, RunContext ctx) throws EntityTransformer.TransformationException {
        String runId = ctx.getRunId();

        try {
            Map<String, Object> transformed = new java.util.HashMap<>(row);
            baseTransformer.resolveAllAnagrafiche(runId, transformed);

            EventsWf event = new EventsWf();

            Instant insertedTsReq = toInstant(firstNonNull(transformed, "INSERTED_TIMESTAMP_REQ", "inserted_timestamp_req", "insertedTimestampReq"));
            Instant insertedTsResp = toInstant(firstNonNull(transformed,
                    "INSERTED_TIMESTAMP_RESP", "inserted_timestamp_resp", "insertedTimestampResp",
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp"));

            // DATE_EVENT
            if (insertedTsResp != null) {
                event.setDateEvent(insertedTsResp.atZone(ZoneOffset.UTC).toLocalDate());
            }

            // Timestamp events
            event.setInsertedTimestampReq(toLocalDateTime(insertedTsReq));
            event.setInsertedTimestampResp(toLocalDateTime(insertedTsResp));

            // Event IDs
            event.setEventIdReq((String) transformed.get("EVENT_ID_REQ"));
            event.setEventIdResp((String) transformed.get("EVENT_ID_RESP"));

            // Outcome
            event.setOutcomeReq((String) transformed.get("OUTCOME_REQ"));
            event.setOutcomeResp((String) transformed.get("OUTCOME_RESP"));

            // Anagrafiche (già risolte in IDs)
            event.setFaultCode((Short) transformed.get("FAULT_CODE"));
            event.setTipoEvento((Short) transformed.get("TIPO_EVENTO"));

            // Implementare regola 7.5: Risolvere FK_TOKENS e FK_POSITION
            String token = (String) transformed.get("TOKEN");
            String nav = (String) transformed.get("NAV");
            String paEmittente = (String) transformed.get("PA_EMITTENTE");
            Boolean isEventMultiPayment = (Boolean) transformed.get("IS_EVENT_MULTI_PAYMENT");

            // 7.5.1: Se TOKEN presente, preferire TOKEN (specie se multi-payment)
            if (token != null && !token.isBlank()) {
                byte[] tokenBytes = token.getBytes();
                Optional<Integer> fkTokensOpt = positionTokensRepository.findLatestByToken(tokenBytes)
                        .map(pt -> pt.getId());

                if (fkTokensOpt.isPresent()) {
                    Integer fkTokens = fkTokensOpt.orElseThrow(
                            () -> new IllegalStateException("FK_TOKENS unexpectedly absent"));
                    event.setFkTokens(fkTokens);
                    log.debug("[{}] [TRANSFORM] EVENTS_WF FK_TOKENS resolved: fkTokens={} token={}",
                            runId, fkTokens, "***");

                    // Derivare FK_POSITION dal TOKEN se necessario
                    // (Nel BulkWriter, dopo insert evento, aggiornare POSITION)
                } else {
                    log.warn("[{}] [TRANSFORM] EVENTS_WF FK_TOKENS NOT FOUND for token", runId);
                }
            } else if (nav != null && paEmittente != null) {
                // 7.5.2: Se TOKEN assente, usare NAV + PA_EMITTENTE per POSITION
                LocalDateTime eventInsertedLdt = toLocalDateTime(insertedTsReq != null ? insertedTsReq : insertedTsResp);

                Optional<Integer> fkPositionOpt = positionRepository
                        .findFirstByNavAndPaEmittenteAndInsertedTimestampLessThanEqualOrderByInsertedTimestampDescIdDesc(
                                nav, paEmittente, eventInsertedLdt)
                        .filter(p -> {
                            long secondsDiff = ChronoUnit.SECONDS.between(p.getInsertedTimestamp(), eventInsertedLdt);
                            return secondsDiff <= 86400; // 24 hours
                        })
                        .map(p -> p.getId());

                if (fkPositionOpt.isPresent()) {
                    Integer fkPosition = fkPositionOpt.orElseThrow(
                            () -> new IllegalStateException("FK_POSITION unexpectedly absent for nav=" + nav));
                    event.setFkPosition(fkPosition);
                    log.debug("[{}] [TRANSFORM] EVENTS_WF FK_POSITION resolved (no TOKEN): fkPosition={} nav={} paEmittente={}",
                            runId, fkPosition, nav, paEmittente);
                } else {
                    log.warn("[{}] [TRANSFORM] EVENTS_WF FK_POSITION NOT FOUND: nav={} paEmittente={} insertedTs={}",
                            runId, nav, paEmittente, insertedTsResp);
                }
            } else {
                log.warn("[{}] [TRANSFORM] EVENTS_WF cannot resolve FK: token={} nav={} paEmittente={}",
                        runId, token, nav, paEmittente);
            }

            return event;
        } catch (Exception e) {
            log.error("[{}] [TRANSFORM] Failed to transform EVENTS_WF: {}", runId, e.getMessage(), e);
            throw new EntityTransformer.TransformationException("EVENTS_WF transform failed", e);
        }
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

    private java.time.LocalDateTime toLocalDateTime(Instant inst) {
        if (inst == null) return null;
        return java.time.LocalDateTime.ofInstant(inst, ZoneOffset.UTC);
    }

    private Object firstNonNull(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Object value = map.get(key);
            if (value == null) {
                continue;
            }
            if (value instanceof String str && str.isBlank()) {
                continue;
            }
            return value;
        }
        return null;
    }
}
