package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
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
 * Trasformer specializzato per POSITION.
 * Regole:
 * - DATE_EVENT da INSERTED_TIMESTAMP (YYYYMMDD).
 * - Verificare se esiste POSITION con (NAV + PA_EMITTENTE) nelle ultime 24 ore.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PositionTransformer {

    private final PositionRepository positionRepository;
    private final EntityTransformerImpl baseTransformer;

    /**
     * Trasformare e validare una POSITION.
     * Regola 7.1: Se esiste POSITION con (NAV + PA_EMITTENTE) nelle 24 ore precedenti,
     * impostare ID per UPDATE; altrimenti INSERT (nuovo).
     */
    public Position transform(Map<String, Object> row, RunContext ctx) throws EntityTransformer.TransformationException {
        String runId = ctx.getRunId();

        try {
            // Risoluzione anagrafiche base
            Map<String, Object> transformed = new java.util.HashMap<>(row);
            baseTransformer.resolveAllAnagrafiche(runId, transformed);

            // Estrarre NAV e PA_EMITTENTE
            String nav = (String) transformed.get("NAV");
            String paEmittente = (String) transformed.get("PA_EMITTENTE");
            Instant insertedTs = toInstant(transformed.get("INSERTED_TIMESTAMP"));

            // Convertire a entità
            Position position = new Position();
            position.setNav(nav);
            position.setPaEmittente(paEmittente);
            position.setLastEvent(null); // Sarà aggiornato da EVENTS_WF
            position.setDateEvents("[]"); // JSON vuoto inizialmente

            // DATE_EVENT da INSERTED_TIMESTAMP
            LocalDate dateEvent = null;
            if (insertedTs != null) {
                dateEvent = insertedTs.atZone(ZoneOffset.UTC).toLocalDate();
                position.setInsertedTimestamp(toLocalDateTime(insertedTs));
            }
            position.setDateEvent(dateEvent);

            // Implementare regola 7.1: verificare finestra 24 ore
            if (nav != null && paEmittente != null && insertedTs != null) {
                // Cercare POSITION con (NAV + PA_EMITTENTE) entro 24 ore precedenti
                java.time.LocalDateTime ldt = toLocalDateTime(insertedTs);
                java.time.LocalDateTime window24hAgo = ldt.minusHours(24);

                // Cercare con query entro finestra temporale
                Optional<Position> existingOpt = findExistingPositionWith24hWindow(nav, paEmittente, insertedTs);
                if (existingOpt.isPresent()) {
                    Position existing = existingOpt.get();
                    position.setId(existing.getId()); // Segnalare UPDATE nel BulkWriter
                    log.debug("[{}] [TRANSFORM] POSITION UPDATE: id={} nav={} paEmittente={} existing_insertedTs={}",
                            runId, existing.getId(), nav, paEmittente, existing.getInsertedTimestamp());
                } else {
                    log.debug("[{}] [TRANSFORM] POSITION INSERT: nav={} paEmittente={} insertedTs={}",
                            runId, nav, paEmittente, insertedTs);
                }
            }

            return position;
        } catch (Exception e) {
            log.error("[{}] [TRANSFORM] Failed to transform POSITION: {}", runId, e.getMessage(), e);
            throw new EntityTransformer.TransformationException("POSITION transform failed", e);
        }
    }

    /**
     * Recuperare POSITION esistente con (NAV + PA_EMITTENTE) entro 24 ore prima di insertedTs.
     */
    private Optional<Position> findExistingPositionWith24hWindow(String nav, String paEmittente, Instant insertedTs) {
        return positionRepository.findFirstByNavAndPaEmittenteAndInsertedTimestampLessThanEqualOrderByInsertedTimestampDescIdDesc(
                nav,
                paEmittente,
                toLocalDateTime(insertedTs)
        ).filter(p -> {
            // Verificare che sia entro 24 ore
            long secondsDiff = java.time.temporal.ChronoUnit.SECONDS.between(
                    p.getInsertedTimestamp(),
                    toLocalDateTime(insertedTs)
            );
            return secondsDiff <= 86400; // 24 hours in seconds
        });
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
}


