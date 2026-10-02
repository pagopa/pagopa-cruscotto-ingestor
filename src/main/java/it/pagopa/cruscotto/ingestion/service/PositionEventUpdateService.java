package it.pagopa.cruscotto.ingestion.service;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTransfersRepository;
import it.pagopa.cruscotto.ingestion.service.ingestion.PositionDateEventsSql;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Servizio per aggiornare POSITION e POSITION_TOKENS dopo l'inserimento di EVENTS_WF.
 * Implementa regola 7.5.3:
 * - Aggiornare POSITION.LAST_EVENT con il timestamp dell'evento
 * - Se la data yyyy-MM-dd dell'evento NON è in POSITION.DATE_EVENTS, aggiungerla
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PositionEventUpdateService {

    private final PositionTokensRepository positionTokensRepository;
    private final PositionTransfersRepository positionTransfersRepository;
    private final AnagraficaService anagraficaService;
    private final JdbcTemplate jdbcTemplate;
    private final DbSchemaConfig dbSchemaConfig;

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

        applyPositionEventUpdates(runId, eventsByPositionId);

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
     * Applica LAST_EVENT e il giorno aggiuntivo in DATE_EVENTS con una UPDATE atomica per riga.
     *
     * <p>Volutamente NON rilegge le POSITION: il job EVENTS_WF e il job POSITION sono JobKey Quartz
     * distinti e girano in parallelo, quindi un read-modify-write dell'array (leggerlo in Java e
     * riscriverlo per intero) perderebbe i giorni aggiunti dall'altro job nel frattempo. Evitare la
     * lettura e' anche l'unico modo sicuro: caricare le entita' qui le renderebbe managed e il dirty
     * checking di Hibernate riproporrebbe la stessa UPDATE distruttiva al commit, anche senza save().</p>
     *
     * <p>L'unione, il dedup, l'ordinamento e l'esclusione del giorno di nascita sono delegati alla
     * UPDATE stessa (vedi {@link PositionDateEventsSql}).</p>
     */
    private void applyPositionEventUpdates(String runId, Map<Integer, List<EventsWf>> eventsByPositionId) {
        if (eventsByPositionId.isEmpty()) {
            return;
        }

        // Ordinamento per positionId: allinea l'ordine di acquisizione dei lock a quello del bulk
        // writer, evitando deadlock quando i due job toccano le stesse righe.
        List<Integer> positionIds = new ArrayList<>(eventsByPositionId.keySet());
        positionIds.sort(Comparator.nullsLast(Integer::compareTo));

        List<PositionDateEventUpdate> updates = new ArrayList<>();
        for (Integer positionId : positionIds) {
            if (positionId == null) {
                continue;
            }
            LocalDateTime maxLastEvent = null;
            Set<LocalDate> eventDates = new TreeSet<>();
            for (EventsWf evt : eventsByPositionId.get(positionId)) {
                if (evt.getInsertedTimestampResp() != null
                        && (maxLastEvent == null || evt.getInsertedTimestampResp().isAfter(maxLastEvent))) {
                    maxLastEvent = evt.getInsertedTimestampResp();
                }
                if (evt.getDateEvent() != null) {
                    eventDates.add(evt.getDateEvent());
                }
            }
            if (eventDates.isEmpty()) {
                // Nessuna data da registrare, ma LAST_EVENT va comunque avanzato.
                updates.add(new PositionDateEventUpdate(positionId, null, maxLastEvent));
            } else {
                for (LocalDate eventDate : eventDates) {
                    updates.add(new PositionDateEventUpdate(positionId, eventDate, maxLastEvent));
                }
            }
        }

        if (updates.isEmpty()) {
            return;
        }

        // NESSUN try/catch attorno alla batchUpdate: in PostgreSQL un errore su uno statement aborta
        // l'intera transazione, e questo metodo gira dentro il @Transactional di
        // updatePositionAfterEvents. Ingoiare l'eccezione lascerebbe proseguire updateTokensFromEvents
        // su una transazione gia' morta ("current transaction is aborted"), con errori fuorvianti e
        // commit fallito. Come nel codice precedente, il fallimento SQL deve propagare e fare rollback.
        int[] applied = jdbcTemplate.batchUpdate(
                PositionDateEventsSql.appendDateEvent(dbSchemaConfig.getSchemaName()),
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        PositionDateEventUpdate update = updates.get(i);
                        ps.setObject(1, update.lastEvent() != null ? Timestamp.valueOf(update.lastEvent()) : null);
                        String iso = update.eventDate() != null ? update.eventDate().toString() : null;
                        ps.setString(2, iso);
                        ps.setString(3, iso);
                        ps.setInt(4, update.positionId());
                    }

                    @Override
                    public int getBatchSize() {
                        return updates.size();
                    }
                });

        for (int i = 0; i < applied.length && i < updates.size(); i++) {
            // pgjdbc restituisce SUCCESS_NO_INFO (-2) quando non puo' contare le righe: solo uno
            // 0 esplicito indica che la POSITION referenziata non esiste.
            if (applied[i] == 0) {
                log.warn("[{}] [EVENT_UPDATE] Position not found: id={} entityName=EVENTS_WF",
                        runId, updates.get(i).positionId());
            }
        }
    }

    /** Un singolo giorno da registrare su una POSITION, con il candidato LAST_EVENT. */
    private record PositionDateEventUpdate(Integer positionId, LocalDate eventDate, LocalDateTime lastEvent) {
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
            boolean changed = setOutcomeIfPresent(token, outcomeReq);

            // Punti 2 e 3 del requisito, esplicitamente NON in AND fra loro: PAYMENT_DATE dipende da
            // OUTCOME_REQ = OK, PAYMENT_METHOD solo dal touchpoint. Annidare il secondo nel primo lo
            // saltava sia con OUTCOME_REQ = KO sia quando PAYMENT_DATE era gia' stata scritta da una
            // SPO precedente.
            if ("OK".equals(outcomeReq) && token.getPaymentDate() == null && insertedTimestampReq != null) {
                token.setPaymentDate(insertedTimestampReq);
                changed = true;
            }
            if ("Touchpoint PSP".equals(token.getTouchpoint())) {
                changed |= setPaymentMethodIfAbsent(token, event.getPaymentMethod());
            }
            return changed;
        }

        if ("KO".equals(outcomeResp)
                && tokenScadutoFaultCodeIds.contains(event.getFaultCode())
                && "Touchpoint PSP".equals(token.getTouchpoint())) {
            boolean changed = setOutcomeIfPresent(token, outcomeReq);

            if ("OK".equals(outcomeReq) && token.getPaymentDate() == null && insertedTimestampReq != null) {
                token.setPaymentDate(insertedTimestampReq);
                changed = true;
            }
            // Punto 3 del requisito, esplicitamente NON in AND con il punto 2: come nel ramo
            // OUTCOME_RESP = OK, PAYMENT_METHOD e' indipendente da PAYMENT_DATE. Touchpoint PSP e' gia'
            // garantito dal guard esterno, quindi qui non va ripetuto.
            changed |= setPaymentMethodIfAbsent(token, event.getPaymentMethod());

            return changed;
        }

        return false;
    }

    /**
     * Scrive PAYMENT_METHOD solo se il token non lo ha gia' valorizzato ("Valorizzare solo se e' null"
     * del requisito SPO): il primo evento che lo porta vince, i successivi non lo riallineano. Un
     * valore nullo in arrivo non azzera quello esistente.
     *
     * @return {@code true} se il token e' stato modificato
     */
    private boolean setPaymentMethodIfAbsent(PositionTokens token, String paymentMethod) {
        if (paymentMethod == null || token.getPaymentMethod() != null) {
            return false;
        }
        token.setPaymentMethod(paymentMethod);
        return true;
    }

    /**
     * Scrive OUTCOME solo se l'evento ne porta uno: il requisito parla del "valore di OUTCOME_REQ
     * presente nell'evento", quindi un evento che non lo valorizza non deve cancellare l'esito gia'
     * registrato. Senza questo guard un SPO senza OUTCOME_REQ azzerava un 'OK' precedente lasciando
     * PAYMENT_DATE valorizzata, cioe' un token con data di incasso ma non incassato.
     */
    private boolean setOutcomeIfPresent(PositionTokens token, String outcomeReq) {
        if (isBlank(outcomeReq) || equalsNullable(token.getOutcome(), outcomeReq)) {
            return false;
        }
        token.setOutcome(outcomeReq);
        return true;
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
