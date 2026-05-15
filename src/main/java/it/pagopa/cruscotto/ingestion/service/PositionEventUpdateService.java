package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Servizio per aggiornare POSITION dopo l'inserimento di EVENTS_WF.
 * Implementa regola 7.5.3:
 * - Aggiornare POSITION.LAST_EVENT con il timestamp dell'evento
 * - Se la data YYYYMMDD dell'evento NON è in POSITION.DATE_EVENTS, aggiungerla
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PositionEventUpdateService {

    private final PositionRepository positionRepository;
    private final JdbcTemplate jdbcTemplate;

    /**
     * Aggiornare POSITION dopo l'inserimento di EVENTS che la referenziano.
     * Regola 7.5.3:
     * - LAST_EVENT = max(LAST_EVENT, evento.INSERTED_TIMESTAMP_RESP)
     * - DATE_EVENTS: aggiungere evento.DATE_EVENT se non presente
     */
    @Transactional
    public void updatePositionAfterEvents(RunContext ctx, List<EventsWf> insertedEvents) {
        if (insertedEvents == null || insertedEvents.isEmpty()) {
            return;
        }

        String runId = ctx.getRunId();

        // Raggruppare eventi per FK_POSITION
        Set<Integer> affectedPositionIds = new HashSet<>();
        for (EventsWf evt : insertedEvents) {
            if (evt.getFkPosition() != null) {
                affectedPositionIds.add(evt.getFkPosition());
            }
        }

        for (Integer positionId : affectedPositionIds) {
            try {
                // Recuperare POSITION
                Optional<Position> posOpt = positionRepository.findById(positionId);
                if (posOpt.isEmpty()) {
                    log.warn("[{}] [EVENT_UPDATE] Position not found: id={}", runId, positionId);
                    continue;
                }

                Position position = posOpt.get();

                // Raccogliere i timestamp e date di tutti gli eventi per questa POSITION
                LocalDateTime maxLastEvent = position.getLastEvent();
                Set<LocalDate> eventDates = parseEventDates(position.getDateEvents());

                for (EventsWf evt : insertedEvents) {
                    if (evt.getFkPosition() != null && evt.getFkPosition().equals(positionId)) {
                        // Aggiornare LAST_EVENT
                        if (evt.getInsertedTimestampResp() != null) {
                            if (maxLastEvent == null || evt.getInsertedTimestampResp().isAfter(maxLastEvent)) {
                                maxLastEvent = evt.getInsertedTimestampResp();
                            }
                        }

                        // Aggiungere EVENT_DATE se non presente
                        if (evt.getDateEvent() != null) {
                            eventDates.add(evt.getDateEvent());
                        }
                    }
                }

                // Aggiornare POSITION
                position.setLastEvent(maxLastEvent);
                position.setDateEvents(serializeEventDates(eventDates));

                positionRepository.save(position);

                log.debug("[{}] [EVENT_UPDATE] Position updated: positionId={} lastEvent={} dateEventsCount={}",
                        runId, positionId, maxLastEvent, eventDates.size());

            } catch (Exception e) {
                log.error("[{}] [EVENT_UPDATE] Failed to update position id={}: {}",
                        runId, positionId, e.getMessage(), e);
                // Non lanciare eccezione: questa è una operazione best-effort post-evento
            }
        }
    }

    /**
     * Parsare jsonb array di date da POSITION.DATE_EVENTS.
     * Formato: ["2026-04-08", "2026-04-09"]
     */
    private Set<LocalDate> parseEventDates(String dateEventsJson) {
        Set<LocalDate> result = new HashSet<>();
        if (dateEventsJson == null || dateEventsJson.isBlank() || "[]".equals(dateEventsJson.trim())) {
            return result;
        }

        try {
            // Semplice parsing manuale per array JSON
            String content = dateEventsJson.trim();
            if (content.startsWith("[") && content.endsWith("]")) {
                content = content.substring(1, content.length() - 1).trim();
                if (!content.isEmpty()) {
                    String[] dates = content.split(",");
                    for (String date : dates) {
                        String dateStr = date.trim().replaceAll("\"", "");
                        if (!dateStr.isEmpty()) {
                            result.add(LocalDate.parse(dateStr));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse DATE_EVENTS: {}", dateEventsJson);
        }

        return result;
    }

    /**
     * Serializzare Set di date a jsonb array.
     * Formato: ["2026-04-08", "2026-04-09"]
     */
    private String serializeEventDates(Set<LocalDate> dates) {
        if (dates == null || dates.isEmpty()) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (LocalDate date : dates.stream().sorted().toList()) {
            if (!first) sb.append(", ");
            sb.append("\"").append(date.toString()).append("\"");
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }
}

