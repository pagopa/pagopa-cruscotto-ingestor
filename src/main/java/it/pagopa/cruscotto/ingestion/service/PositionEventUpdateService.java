package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTransfersRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Servizio per aggiornare POSITION e POSITION_TOKENS dopo l'inserimento di EVENTS_WF.
 * Implementa regola 7.5.3:
 * - Aggiornare POSITION.LAST_EVENT con il timestamp dell'evento
 * - Se la data YYYYMMDD dell'evento NON è in POSITION.DATE_EVENTS, aggiungerla
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PositionEventUpdateService {

    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.BASIC_ISO_DATE;

    private final PositionRepository positionRepository;
    private final PositionTokensRepository positionTokensRepository;
    private final PositionTransfersRepository positionTransfersRepository;
    private final AnagraficaService anagraficaService;

    /**
     * Aggiornare POSITION e POSITION_TOKENS dopo l'inserimento di EVENTS che li referenziano.
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
        Map<Integer, List<EventsWf>> eventsByPositionId = new HashMap<>();
        boolean hasTokenLinkedEvents = false;
        for (EventsWf evt : insertedEvents) {
            if (evt.getFkPosition() != null) {
                eventsByPositionId.computeIfAbsent(evt.getFkPosition(), ignored -> new ArrayList<>()).add(evt);
            }
            if (evt.getFkTokens() != null) {
                hasTokenLinkedEvents = true;
            }
        }

        Map<Integer, Position> positionsById = new HashMap<>();
        if (!eventsByPositionId.isEmpty()) {
            List<Position> positions = positionRepository.findAllById(eventsByPositionId.keySet());
            for (Position position : positions) {
                if (position.getId() != null) {
                    positionsById.put(position.getId(), position);
                }
            }
        }

        List<Position> changedPositions = new ArrayList<>();
        for (Map.Entry<Integer, List<EventsWf>> positionEntry : eventsByPositionId.entrySet()) {
            Integer positionId = positionEntry.getKey();
            try {
                Position position = positionsById.get(positionId);
                if (position == null) {
                    log.warn("[{}] [EVENT_UPDATE] Position not found: id={}", runId, positionId);
                    continue;
                }

                // Raccogliere i timestamp e date di tutti gli eventi per questa POSITION
                LocalDateTime maxLastEvent = position.getLastEvent();
                Set<LocalDate> eventDates = parseEventDates(position.getDateEvents());
                LocalDate positionDate = position.getDateEvent();

                for (EventsWf evt : positionEntry.getValue()) {
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

                // Rule 7.5.3: DATE_EVENTS contains associated EVENT dates only, never the POSITION date itself.
                if (positionDate != null) {
                    eventDates.remove(positionDate);
                }

                // Aggiornare POSITION
                position.setLastEvent(maxLastEvent);
                position.setDateEvents(serializeEventDates(eventDates));
                changedPositions.add(position);

                log.debug("[{}] [EVENT_UPDATE] Position updated: positionId={} lastEvent={} dateEventsCount={}",
                        runId, positionId, maxLastEvent, eventDates.size());

            } catch (Exception e) {
                log.error("[{}] [EVENT_UPDATE] Failed to update position id={}: {}",
                        runId, positionId, e.getMessage(), e);
                // Non lanciare eccezione: questa è una operazione best-effort post-evento
            }
        }
        if (!changedPositions.isEmpty()) {
            changedPositions.sort(Comparator.comparing(Position::getId, Comparator.nullsLast(Integer::compareTo)));
            positionRepository.saveAll(changedPositions);
        }

        if (hasTokenLinkedEvents) {
            Set<Short> sendPaymentOutcomeEventIds = resolveSendPaymentOutcomeEventIds(runId);
            Set<Short> activatePaymentNoticeEventIds = resolveActivatePaymentNoticeEventIds(runId);
            Set<Short> pspNotifyPaymentEventIds = resolvePspNotifyPaymentEventIds(runId);
            Set<Short> closePaymentEventIds = resolveClosePaymentEventIds(runId);
            Set<Short> tokenScadutoFaultCodeIds = resolveTokenScadutoFaultCodeIds(runId);
            Map<Integer, List<EventsWf>> eventsByTokenId = new HashMap<>();
            for (EventsWf evt : insertedEvents) {
                if (evt.getFkTokens() != null
                        && (sendPaymentOutcomeEventIds.contains(evt.getTipoEvento())
                        || activatePaymentNoticeEventIds.contains(evt.getTipoEvento())
                        || pspNotifyPaymentEventIds.contains(evt.getTipoEvento())
                        || closePaymentEventIds.contains(evt.getTipoEvento()))) {
                    eventsByTokenId.computeIfAbsent(evt.getFkTokens(), ignored -> new ArrayList<>()).add(evt);
                }
            }
            updateTokensFromEvents(runId, eventsByTokenId, sendPaymentOutcomeEventIds,
                    activatePaymentNoticeEventIds, pspNotifyPaymentEventIds, closePaymentEventIds,
                    tokenScadutoFaultCodeIds);
        }
    }

    /**
     * Parsare jsonb array di date da POSITION.DATE_EVENTS.
     * Formati supportati: ["20260408", "20260409"] oppure ["2026-04-08", "2026-04-09"].
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
                            result.add(parseDateValue(dateStr));
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
     * Formato: ["20260408", "20260409"]
     */
    private String serializeEventDates(Set<LocalDate> dates) {
        if (dates == null || dates.isEmpty()) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        for (LocalDate date : dates.stream().sorted().toList()) {
            if (!first) sb.append(", ");
            sb.append("\"").append(YYYYMMDD.format(date)).append("\"");
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }

    private LocalDate parseDateValue(String value) {
        String trimmed = value == null ? "" : value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Blank date value");
        }
        if (trimmed.length() == 8 && trimmed.chars().allMatch(Character::isDigit)) {
            return LocalDate.parse(trimmed, YYYYMMDD);
        }
        return LocalDate.parse(trimmed);
    }

    private void updateTokensFromEvents(String runId,
                                        Map<Integer, List<EventsWf>> eventsByTokenId,
                                        Set<Short> sendPaymentOutcomeEventIds,
                                        Set<Short> activatePaymentNoticeEventIds,
                                        Set<Short> pspNotifyPaymentEventIds,
                                        Set<Short> closePaymentEventIds,
                                        Set<Short> tokenScadutoFaultCodeIds) {
        if (eventsByTokenId.isEmpty()) {
            return;
        }

        Map<Integer, PositionTokens> tokensById = new HashMap<>();
        List<PositionTokens> tokens = positionTokensRepository.findAllById(eventsByTokenId.keySet());
        for (PositionTokens token : tokens) {
            if (token.getId() != null) {
                tokensById.put(token.getId(), token);
            }
        }

        Map<Integer, List<PositionTransfers>> transfersByTokenId = new HashMap<>();
        List<PositionTransfers> transfers = positionTransfersRepository.findByFkTokenInOrderByFkTokenAscIdDesc(eventsByTokenId.keySet());
        for (PositionTransfers transfer : transfers) {
            if (transfer.getFkToken() != null) {
                transfersByTokenId.computeIfAbsent(transfer.getFkToken(), ignored -> new ArrayList<>()).add(transfer);
            }
        }

        List<PositionTokens> changedTokens = new ArrayList<>();
        List<PositionTransfers> changedTransfers = new ArrayList<>();

        for (Map.Entry<Integer, List<EventsWf>> tokenEntry : eventsByTokenId.entrySet()) {
            Integer tokenId = tokenEntry.getKey();
            if (tokenId == null) {
                continue;
            }

            PositionTokens token = tokensById.get(tokenId);
            if (token == null) {
                log.warn("[{}] [EVENT_UPDATE] Position token not found: id={}", runId, tokenId);
                continue;
            }
            List<PositionTransfers> tokenTransfers = new ArrayList<>(transfersByTokenId.getOrDefault(tokenId, List.of()));

            List<EventsWf> sortedEvents = new ArrayList<>(tokenEntry.getValue());
            sortedEvents.sort(Comparator.comparing(
                    this::eventOrderTimestamp,
                    Comparator.nullsLast(LocalDateTime::compareTo))
            );

            boolean tokenChanged = false;
            boolean transfersChanged = false;
            for (EventsWf event : sortedEvents) {
                Short eventTypeId = event.getTipoEvento();
                if (sendPaymentOutcomeEventIds.contains(eventTypeId)) {
                    tokenChanged |= applySendPaymentOutcomeRules(token, event, tokenScadutoFaultCodeIds);
                }
                if (activatePaymentNoticeEventIds.contains(eventTypeId)) {
                    tokenChanged |= applyActivatePaymentNoticeRules(token, event);
                }
                if (pspNotifyPaymentEventIds.contains(eventTypeId)) {
                    PspNotifyUpdateResult updateResult = applyPspNotifyPaymentRules(token, tokenTransfers, event);
                    tokenChanged |= updateResult.tokenChanged();
                    transfersChanged |= updateResult.transfersChanged();
                }
                if (closePaymentEventIds.contains(eventTypeId)) {
                    tokenChanged |= applyClosePaymentRules(token, event);
                }
            }

            if (tokenChanged) {
                changedTokens.add(token);
                log.debug("[{}] [EVENT_UPDATE] Position token updated from events: tokenId={} outcome={} paymentDate={} paymentMethod={}",
                        runId, tokenId, token.getOutcome(), token.getPaymentDate(), token.getPaymentMethod());
            }
            if (transfersChanged) {
                changedTransfers.addAll(tokenTransfers);
                log.debug("[{}] [EVENT_UPDATE] Position transfers updated from events: tokenId={} transferCount={}",
                        runId, tokenId, tokenTransfers.size());
            }
        }

        if (!changedTokens.isEmpty()) {
            changedTokens.sort(Comparator.comparing(PositionTokens::getId, Comparator.nullsLast(Integer::compareTo)));
            positionTokensRepository.saveAll(changedTokens);
        }
        if (!changedTransfers.isEmpty()) {
            changedTransfers.sort(Comparator.comparing(PositionTransfers::getId, Comparator.nullsLast(Integer::compareTo)));
            positionTransfersRepository.saveAll(changedTransfers);
        }
    }

    private boolean applySendPaymentOutcomeRules(PositionTokens token,
                                                 EventsWf event,
                                                 Set<Short> tokenScadutoFaultCodeIds) {
        String outcomeResp = event.getOutcomeResp();
        String outcomeReq = event.getOutcomeReq();
        LocalDateTime insertedTimestampReq = event.getInsertedTimestampReq();

        if ("OK".equals(outcomeResp)) {
            boolean changed = !equalsNullable(token.getOutcome(), outcomeReq);
            token.setOutcome(outcomeReq);

            if ("OK".equals(outcomeReq) && token.getPaymentDate() == null && insertedTimestampReq != null) {
                token.setPaymentDate(insertedTimestampReq);
                changed = true;
                if ("Touchpoint PSP".equals(token.getTouchpoint())
                        && !equalsNullable(token.getPaymentMethod(), event.getPaymentMethod())) {
                    token.setPaymentMethod(event.getPaymentMethod());
                    changed = true;
                }
            }
            return changed;
        }

        if ("KO".equals(outcomeResp)
                && tokenScadutoFaultCodeIds.contains(event.getFaultCode())
                && "Touchpoint PSP".equals(token.getTouchpoint())) {
            boolean changed = !equalsNullable(token.getOutcome(), outcomeReq);
            token.setOutcome(outcomeReq);

            if ("OK".equals(outcomeReq) && token.getPaymentDate() == null && insertedTimestampReq != null) {
                token.setPaymentDate(insertedTimestampReq);
                changed = true;
            }

            if (!equalsNullable(token.getPaymentMethod(), event.getPaymentMethod())) {
                token.setPaymentMethod(event.getPaymentMethod());
                changed = true;
            }
            return changed;
        }

        return false;
    }

    private boolean applyActivatePaymentNoticeRules(PositionTokens token, EventsWf event) {
        if (!"OK".equals(event.getOutcomeResp())) {
            return false;
        }

        String eventCreditorRefId = event.getCreditorRefId();
        String desiredCreditorRefId = null;
        if (eventCreditorRefId != null && !eventCreditorRefId.equals(token.getIuv())) {
            desiredCreditorRefId = eventCreditorRefId;
        }

        if (equalsNullable(token.getCreditorRefId(), desiredCreditorRefId)) {
            return false;
        }
        token.setCreditorRefId(desiredCreditorRefId);
        return true;
    }

    private PspNotifyUpdateResult applyPspNotifyPaymentRules(PositionTokens token,
                                                             List<PositionTransfers> transfers,
                                                             EventsWf event) {
        if ("OK".equals(event.getOutcomeResp())) {
            boolean tokenChanged = false;
            if (!equalsNullable(token.getPsp(), event.getPsp())) {
                token.setPsp(event.getPsp());
                tokenChanged = true;
            }
            if (!equalsNullable(token.getIntermediarioPsp(), event.getIntermediarioPsp())) {
                token.setIntermediarioPsp(event.getIntermediarioPsp());
                tokenChanged = true;
            }
            if (!equalsNullable(token.getCanale(), event.getCanale())) {
                token.setCanale(event.getCanale());
                tokenChanged = true;
            }

            boolean transfersChanged = false;
            for (PositionTransfers transfer : transfers) {
                boolean transferChanged = false;
                if (!equalsNullable(transfer.getPsp(), event.getPsp())) {
                    transfer.setPsp(event.getPsp());
                    transferChanged = true;
                }
                if (!equalsNullable(transfer.getIntermediarioPsp(), event.getIntermediarioPsp())) {
                    transfer.setIntermediarioPsp(event.getIntermediarioPsp());
                    transferChanged = true;
                }
                if (!equalsNullable(transfer.getCanale(), event.getCanale())) {
                    transfer.setCanale(event.getCanale());
                    transferChanged = true;
                }
                transfersChanged |= transferChanged;
            }
            return new PspNotifyUpdateResult(tokenChanged, transfersChanged);
        }

        if ("KO".equals(event.getOutcomeResp()) && isBlank(token.getOutcome())) {
            token.setOutcome("KO");
            return new PspNotifyUpdateResult(true, false);
        }

        return new PspNotifyUpdateResult(false, false);
    }

    private boolean applyClosePaymentRules(PositionTokens token, EventsWf event) {
        String outcomeReq = event.getOutcomeReq();
        String outcomeResp = event.getOutcomeResp();

        if ("OK".equals(outcomeReq) && "OK".equals(outcomeResp)) {
            boolean changed = false;
            if (!equalsNullable(token.getPaymentMethod(), event.getPaymentMethod())) {
                token.setPaymentMethod(event.getPaymentMethod());
                changed = true;
            }
            if (!equalsNullable(token.getPsp(), event.getPsp())) {
                token.setPsp(event.getPsp());
                changed = true;
            }
            if (!equalsNullable(token.getIntermediarioPsp(), event.getIntermediarioPsp())) {
                token.setIntermediarioPsp(event.getIntermediarioPsp());
                changed = true;
            }
            if (!equalsNullable(token.getCanale(), event.getCanale())) {
                token.setCanale(event.getCanale());
                changed = true;
            }
            return changed;
        }

        if ("KO".equals(outcomeReq) && "OK".equals(outcomeResp) && isBlank(token.getOutcome())) {
            token.setOutcome("KO");
            return true;
        }

        return false;
    }

    private Set<Short> resolveSendPaymentOutcomeEventIds(String runId) {
        Set<Short> eventIds = new HashSet<>(resolveEventIdsByName(runId, "sendPaymentOutcome"));
        eventIds.addAll(resolveEventIdsByName(runId, "sendPaymentOutcomeV2"));
        return eventIds;
    }

    private Set<Short> resolveActivatePaymentNoticeEventIds(String runId) {
        Set<Short> eventIds = new HashSet<>(resolveEventIdsByName(runId, "activatePaymentNotice"));
        eventIds.addAll(resolveEventIdsByName(runId, "activatePaymentNoticeV2"));
        return eventIds;
    }

    private Set<Short> resolvePspNotifyPaymentEventIds(String runId) {
        Set<Short> eventIds = new HashSet<>(resolveEventIdsByName(runId, "pspNotifyPayment"));
        eventIds.addAll(resolveEventIdsByName(runId, "pspNotifyPaymentV2"));
        return eventIds;
    }

    private Set<Short> resolveClosePaymentEventIds(String runId) {
        Set<Short> eventIds = new HashSet<>(resolveEventIdsByName(runId, "closePayment"));
        eventIds.addAll(resolveEventIdsByName(runId, "closePayment-v2"));
        return eventIds;
    }

    private Set<Short> resolveTokenScadutoFaultCodeIds(String runId) {
        Set<Short> faultCodes = new HashSet<>();
        faultCodes.add((short) anagraficaService.resolveFaultCodeId(runId, "PPT_TOKEN_SCADUTO"));
        faultCodes.add((short) anagraficaService.resolveFaultCodeId(runId, "PPT_TOKEN_SCADUTO_KO"));
        return faultCodes;
    }

    private Set<Short> resolveEventIdsByName(String runId, String eventName) {
        Set<Short> eventIds = new HashSet<>();
        eventIds.add((short) anagraficaService.resolveEventoId(runId, eventName, ""));
        eventIds.add((short) anagraficaService.resolveEventoId(runId, eventName, "REQ/RESP"));
        return eventIds;
    }

    private LocalDateTime eventOrderTimestamp(EventsWf event) {
        if (event.getInsertedTimestampReq() != null) {
            return event.getInsertedTimestampReq();
        }
        if (event.getInsertedTimestampResp() != null) {
            return event.getInsertedTimestampResp();
        }
        if (event.getDateEvent() != null) {
            return event.getDateEvent().atStartOfDay();
        }
        return null;
    }

    private boolean equalsNullable(String first, String second) {
        if (first == null) {
            return second == null;
        }
        return first.equals(second);
    }

    private boolean equalsNullable(Short first, Short second) {
        if (first == null) {
            return second == null;
        }
        return first.equals(second);
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private record PspNotifyUpdateResult(boolean tokenChanged, boolean transfersChanged) {
    }
}
