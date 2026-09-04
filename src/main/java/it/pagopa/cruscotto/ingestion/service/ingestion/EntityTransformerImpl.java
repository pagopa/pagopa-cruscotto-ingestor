package it.pagopa.cruscotto.ingestion.service.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.repository.PositionRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTokenRegistryReader;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.service.AnagraficaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Trasformer di dominio: converte dati grezzi ADX in entità di dominio SERT.
 * Applica regole di mapping, risoluzione anagrafiche, e trasformazioni di valore.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EntityTransformerImpl implements EntityTransformer {

    private final ObjectMapper objectMapper;
    private final AnagraficaService anagraficaService;
    private final PositionRepository positionRepository;
    private final PositionTokensRepository positionTokensRepository;
    private final CanonicalTokenResolver canonicalTokenResolver;

    @Override
    public <T> T transform(Map<String, Object> row, Class<T> targetClass) throws TransformationException {
        try {
            return transform(row, targetClass, null, null);
        } catch (Exception e) {
            String errorMsg = "Failed to transform row to " + targetClass.getSimpleName();
            log.error(errorMsg, e);
            throw new TransformationException(errorMsg, e);
        }
    }

    /**
     * Transform overload con RunContext e EntityName per logica specializzata.
     */
    @Override
    public <T> T transform(Map<String, Object> row, Class<T> targetClass,
                           RunContext ctx, EntityName entity) throws TransformationException {
        try {
            Map<String, Object> transformed = new HashMap<>(row);
            String runId = ctx != null ? ctx.getRunId() : "unknown";

            // Risolvere tutte le anagrafiche (indipendentemente dall'entità)
            long anagraficaStartNs = System.nanoTime();
            try {
                resolveAllAnagrafiche(ctx, runId, transformed);
            } finally {
                if (ctx != null) {
                    ctx.addAnagraficaDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - anagraficaStartNs));
                }
            }

            normalizeTargetFields(transformed, targetClass, ctx, entity);
            validateRequiredForeignKeys(ctx, entity, transformed);

            // TODO: Applicare regole specifiche per entità
            // - POSITION: data_event derivata, last_event, date_events
            // - POSITION_TOKENS: regole evento (sendPaymentOutcome, activatePaymentNotice, etc.)
            // - POSITION_TRANSFERS: FK_TOKEN via POSITION_TOKENS
            // - EVENTS_WF: last_event, date_events su POSITION
            // - EXTRA_INFO: basic insert

            // Convertire la mappa trasformata in entità
            return objectMapper.convertValue(transformed, targetClass);
        } catch (TransformationException e) {
            throw e;
        } catch (Exception e) {
            String errorMsg = "Failed to transform row to " + targetClass.getSimpleName();
            log.error(errorMsg, e);
            throw new TransformationException(errorMsg, e);
        }
    }

    /**
     * Risolvere tutti i campi stringa delle anagrafiche tramite AnagraficaService.
     */
    public void resolveAllAnagrafiche(String runId, Map<String, Object> transformed) {
        resolveAllAnagrafiche(null, runId, transformed);
    }

    private void resolveAllAnagrafiche(RunContext ctx, String runId, Map<String, Object> transformed) {
        // STAZIONE
        String stazioneCodice = getStringValue(transformed, "STAZIONE");
        if (stazioneCodice != null) {
            if (ctx != null) {
                ctx.incrementAnagraficaLookupCount();
            }
            Short stazioneId = anagraficaService.resolveStazione(runId, stazioneCodice);
            transformed.put("STAZIONE", stazioneId);
        }

        // CANALE
        String canaleCodice = getStringValue(transformed, "CANALE");
        if (canaleCodice != null) {
            if (ctx != null) {
                ctx.incrementAnagraficaLookupCount();
            }
            Short canaleId = anagraficaService.resolveCanale(runId, canaleCodice);
            transformed.put("CANALE", canaleId);
        }

        // PSP
        String pspCodice = getStringValue(transformed, "PSP");
        if (pspCodice != null) {
            if (ctx != null) {
                ctx.incrementAnagraficaLookupCount();
            }
            Short pspId = anagraficaService.resolvePsp(runId, pspCodice);
            transformed.put("PSP", pspId);
        }

        String paEmittenteCodice = getStringValueByKeys(transformed, "PA_EMITTENTE", "pa_emittente", "paEmittente");
        if (paEmittenteCodice != null) {
            if (ctx != null) {
                ctx.incrementAnagraficaLookupCount();
            }
            anagraficaService.resolvePaEmittenteId(runId, paEmittenteCodice);
        }

        // INTERMEDIARIO_PA
        String intermediarioPaCodice = getStringValue(transformed, "INTERMEDIARIO_PA");
        if (intermediarioPaCodice != null) {
            if (ctx != null) {
                ctx.incrementAnagraficaLookupCount();
            }
            Short intermediarioPaId = anagraficaService.resolveIntermediarioPa(runId, intermediarioPaCodice);
            transformed.put("INTERMEDIARIO_PA", intermediarioPaId);
        }

        // INTERMEDIARIO_PSP
        String intermediarioPspCodice = getStringValue(transformed, "INTERMEDIARIO_PSP");
        if (intermediarioPspCodice != null) {
            if (ctx != null) {
                ctx.incrementAnagraficaLookupCount();
            }
            Short intermediarioPspId = anagraficaService.resolveIntermediarioPsp(runId, intermediarioPspCodice);
            transformed.put("INTERMEDIARIO_PSP", intermediarioPspId);
        }

        // FAULT_CODE
        String faultCodeCodice = getStringValue(transformed, "FAULT_CODE");
        if (faultCodeCodice != null) {
            if (ctx != null) {
                ctx.incrementAnagraficaLookupCount();
            }
            transformed.put("FAULT_CODE_RAW", faultCodeCodice);
            Short faultCodeId = anagraficaService.resolveFaultCode(runId, faultCodeCodice);
            transformed.put("FAULT_CODE", faultCodeId);
        }

        // TIPO_EVENTO + SOTTO_TIPO_EVENTO → risolti come coppia in ANAG_EVENTO
        String tipoEventoCodice = getStringValue(transformed, "TIPO_EVENTO");
        String sottoTipoEventoCodice = getStringValue(transformed, "SOTTO_TIPO_EVENTO");
        if (tipoEventoCodice != null) {
            transformed.put("TIPO_EVENTO_RAW", tipoEventoCodice);
        }
        if (sottoTipoEventoCodice != null) {
            transformed.put("SOTTO_TIPO_EVENTO_RAW", sottoTipoEventoCodice);
        }
        if (tipoEventoCodice != null) {
            if (ctx != null) {
                ctx.incrementAnagraficaLookupCount();
            }
            Short eventoId = (short) anagraficaService.resolveEventoId(runId, tipoEventoCodice,
                    sottoTipoEventoCodice != null ? sottoTipoEventoCodice : "");
            transformed.put("TIPO_EVENTO", eventoId);
            // SOTTO_TIPO_EVENTO non è una colonna nelle entità di dominio → rimuovere per sicurezza
            transformed.remove("SOTTO_TIPO_EVENTO");
        }

        // Cast numerici espliciti
        castNumericFields(transformed);
    }

    private <T> void normalizeTargetFields(Map<String, Object> transformed, Class<T> targetClass,
                                           RunContext ctx, EntityName entity) {
        if (targetClass.getSimpleName().equals("Position")) {
            LocalDateTime insertedTs = toLocalDateTime(firstNonNull(transformed,
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp"));
            LocalDateTime lastEventTs = toLocalDateTime(firstNonNull(transformed,
                    "LAST_EVENT", "last_event", "lastEvent"));
            String nav = getStringValueByKeys(transformed, "NAV", "nav");
            String paEmittente = getStringValueByKeys(transformed, "PA_EMITTENTE", "pa_emittente", "paEmittente");

            transformed.put("dateEvent", toLocalDate(firstNonNull(transformed,
                    "DATE_EVENT", "date_event", "dateEvent", "INSERTED_TIMESTAMP", "inserted_timestamp")));
            transformed.put("insertedTimestamp", insertedTs);
            transformed.put("lastEvent", lastEventTs != null ? lastEventTs : insertedTs);
            transformed.put("nav", nav);
            transformed.put("paEmittente", paEmittente);
            transformed.put("dateEvents", firstNonNull(transformed, "DATE_EVENTS", "date_events", "dateEvents") != null
                    ? getStringValueByKeys(transformed, "DATE_EVENTS", "date_events", "dateEvents")
                    : "[]");

            // Rule 7.1: if POSITION with same (NAV + PA_EMITTENTE) exists in [ts-24h, ts], mark as UPDATE.
            BatchLocalCache batchCache = ctx != null ? ctx.getBatchLocalCache() : null;
            Integer existingPositionId = resolveExistingPositionId(ctx, nav, paEmittente, insertedTs, batchCache);
            if (existingPositionId != null) {
                transformed.put("id", existingPositionId);
            }

            // Map anagrafica IDs (resolved by resolveAllAnagrafiche)
            copyAnagraficaFields(transformed);
            return;
        }

        if (targetClass.getSimpleName().equals("PositionTokens")) {
            LocalDate dateEvent = resolveDateEventWithFallback(transformed,
                    "DATE_EVENT", "date_event", "dateEvent",
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp",
                    "PAYMENT_DATE", "payment_date", "paymentDate");
            LocalDateTime sourceInsertedTs = toLocalDateTime(firstNonNull(transformed,
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp",
                    "PAYMENT_DATE", "payment_date", "paymentDate"));
            transformed.put("dateEvent", dateEvent);
            byte[] tokenBytes = toByteArray(firstNonNull(transformed, "TOKEN", "token"));
            transformed.put("token", tokenBytes);
            transformed.put("amount", toBigDecimal(firstNonNull(transformed, "AMOUNT", "amount")));

            // Detailed FEE diagnostic
            Object feeRaw = firstNonNull(transformed, "FEE", "fee");
            BigDecimal feeValue = toBigDecimal(feeRaw);
            if (feeRaw != null && feeValue == null) {
                log.warn("FEE conversion failed: raw={} (type={}), resulting in null", feeRaw, feeRaw.getClass().getSimpleName());
            }
            transformed.put("fee", feeValue);

            transformed.put("iuv", getStringValueByKeys(transformed, "IUV", "iuv"));
            transformed.put("creditorRefId", getStringValueByKeys(transformed, "CREDITOR_REF_ID", "creditor_ref_id", "creditorRefId"));
            transformed.put("outcome", getStringValueByKeys(transformed, "OUTCOME", "outcome"));
            transformed.put("idCarrello", getStringValueByKeys(transformed, "ID_CARRELLO", "id_carrello", "idCarrello"));
            transformed.put("touchpoint", getStringValueByKeys(transformed, "TOUCHPOINT", "touchpoint"));
            transformed.put("paymentMethod", getStringValueByKeys(transformed, "PAYMENT_METHOD", "payment_method", "paymentMethod"));
            transformed.put("paymentDate", toLocalDateTime(firstNonNull(transformed, "PAYMENT_DATE", "payment_date", "paymentDate", "INSERTED_TIMESTAMP", "inserted_timestamp")));

            // Map anagrafica IDs (resolved by resolveAllAnagrafiche)
            copyAnagraficaFields(transformed);

            Integer fkPosition = resolvePositionFk(ctx, entity, transformed, dateEvent, sourceInsertedTs);
            transformed.put("fkPosition", fkPosition);

            // First-write-wins: never update existing POSITION_TOKENS rows.
            applyPositionTokenEventRules(transformed, null);
            return;
        }

        if (targetClass.getSimpleName().equals("PositionTransfers")) {
            LocalDate dateEvent = resolveDateEventWithFallback(transformed,
                    "DATE_EVENT", "date_event", "dateEvent",
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp");
            transformed.put("dateEvent", dateEvent);
            transformed.put("paTransfer", getStringValueByKeys(transformed, "PA_TRANSFER", "pa_transfer", "paTransfer"));
            transformed.put("idTransfer", toShort(firstNonNull(transformed, "ID_TRANSFER", "id_transfer", "idTransfer")));
            transformed.put("ibanTransfer", getStringValueByKeys(transformed, "IBAN_TRANSFER", "iban_transfer", "ibanTransfer"));
            transformed.put("amountTransfer", toBigDecimal(firstNonNull(transformed, "TRANSFER_AMOUNT", "AMOUNT_TRANSFER", "amount_transfer", "amountTransfer")));
            transformed.put("isBollo", toBoolean(firstNonNull(transformed, "IS_BOLLO", "is_bollo", "isBollo")));

            // Map anagrafica IDs (resolved by resolveAllAnagrafiche)
            copyAnagraficaFields(transformed);

            transformed.put("fkToken", resolveTokenFkByTokenOnly(ctx, entity, transformed));

            return;
        }

        if (targetClass.getSimpleName().equals("ExtraInfo")) {
            LocalDate dateEvent = resolveDateEventWithFallback(transformed,
                    "DATE_EVENT", "date_event", "dateEvent",
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp");
            LocalDateTime sourceInsertedTs = toLocalDateTime(firstNonNull(transformed,
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp"));
            transformed.put("dateEvent", dateEvent);
            transformed.put("infoName", getStringValueByKeys(transformed, "INFO_NAME", "info_name", "infoName", "TRANSACTION_STATUS", "transaction_status"));
            transformed.put("infoValue", getStringValueByKeys(transformed, "INFO_VALUE", "info_value", "infoValue", "ADDITIONAL_INFO", "additional_info"));

            // Map TIPO_EVENTO ID (already resolved by resolveAllAnagrafiche)
            Object tipoEventoValue = transformed.get("TIPO_EVENTO");
            if (tipoEventoValue instanceof Short) {
                transformed.put("tipoEvento", tipoEventoValue);
            }

            // Map anagrafica IDs (resolved by resolveAllAnagrafiche)
            copyAnagraficaFields(transformed);

            Integer fkPosition = resolvePositionFk(ctx, entity, transformed, dateEvent, sourceInsertedTs);
            transformed.put("fkToken", resolveTokenFk(ctx, entity, transformed, dateEvent, fkPosition, true));
            return;
        }

        if (targetClass.getSimpleName().equals("EventsWf")) {
            LocalDate dateEvent = resolveDateEventWithFallback(transformed,
                    "DATE_EVENT", "date_event", "dateEvent",
                    "INSERTED_TIMESTAMP_RESP", "inserted_timestamp_resp", "insertedTimestampResp",
                    "INSERTED_TIMESTAMP_REQ", "inserted_timestamp_req", "insertedTimestampReq",
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp");
            LocalDateTime sourceInsertedTs = toLocalDateTime(firstNonNull(transformed,
                    "INSERTED_TIMESTAMP_RESP", "inserted_timestamp_resp", "insertedTimestampResp",
                    "INSERTED_TIMESTAMP_REQ", "inserted_timestamp_req", "insertedTimestampReq",
                    "INSERTED_TIMESTAMP", "inserted_timestamp", "insertedTimestamp"));
            transformed.put("dateEvent", dateEvent);
            transformed.put("insertedTimestampReq", toLocalDateTime(firstNonNull(transformed, "INSERTED_TIMESTAMP_REQ", "inserted_timestamp_req", "insertedTimestampReq")));
            transformed.put("insertedTimestampResp", toLocalDateTime(firstNonNull(transformed, "INSERTED_TIMESTAMP_RESP", "inserted_timestamp_resp", "insertedTimestampResp", "INSERTED_TIMESTAMP", "inserted_timestamp")));
            transformed.put("eventIdReq", getStringValueByKeys(transformed, "EVENT_ID_REQ", "event_id_req", "eventIdReq"));
            transformed.put("eventIdResp", getStringValueByKeys(transformed, "EVENT_ID_RESP", "event_id_resp", "eventIdResp"));

            // Map FAULT_CODE and TIPO_EVENTO IDs (already resolved by resolveAllAnagrafiche)
            Object faultCodeValue = transformed.get("FAULT_CODE");
            if (faultCodeValue instanceof Short) {
                transformed.put("faultCode", faultCodeValue);
            }
            Object tipoEventoValue = transformed.get("TIPO_EVENTO");
            if (tipoEventoValue instanceof Short) {
                transformed.put("tipoEvento", tipoEventoValue);
            }

            transformed.put("outcomeReq", getStringValueByKeys(transformed, "OUTCOME_REQ", "outcome_req", "outcomeReq", "OUTCOME", "outcome"));
            transformed.put("outcomeResp", getStringValueByKeys(transformed, "OUTCOME_RESP", "outcome_resp", "outcomeResp"));
            transformed.put("creditorRefId", getStringValueByKeys(transformed, "CREDITOR_REF_ID", "creditor_ref_id", "creditorRefId"));
            transformed.put("paymentMethod", getStringValueByKeys(transformed, "PAYMENT_METHOD", "payment_method", "paymentMethod"));

            // Map anagrafica IDs (resolved by resolveAllAnagrafiche)
            copyAnagraficaFields(transformed);

            boolean isMultiPayment = Boolean.TRUE.equals(toBoolean(firstNonNull(transformed,
                    "IS_EVENT_MULTI_PAYMENT", "is_event_multi_payment", "isEventMultiPayment")));

            Integer fkPosition;
            Integer fkToken;
            TokenResolution canonicalTokenResolution = resolveCanonicalTokenResolution(ctx, transformed);
            fkToken = canonicalTokenResolution.fkToken();
            fkPosition = canonicalTokenResolution.fkPosition();

            if (isMultiPayment) {
                // Multi-payment: prefer TOKEN and derive POSITION from the same token row.
                if (fkToken == null) {
                    fkToken = resolveTokenFk(ctx, entity, transformed, dateEvent, null, false);
                }
                if (fkPosition == null && fkToken != null) {
                    fkPosition = resolvePositionFromTokenId(ctx, fkToken);
                }
            } else {
                // Non multi-payment: token-first, then fallback to POSITION business key.
                if (fkPosition == null) {
                    fkPosition = resolvePositionFk(ctx, entity, transformed, dateEvent, sourceInsertedTs);
                }
                // Resolve FK_TOKENS only when the ADX row actually carries a TOKEN, and
                // resolve it strictly by TOKEN value. Events whose TOKEN is absent from
                // POSITION_TOKENS must keep FK_TOKENS null instead of being linked to an
                // arbitrary token that merely shares the same POSITION + IUV.
                if (fkToken == null && toByteArray(firstNonNull(transformed, "TOKEN", "token")) != null) {
                    fkToken = resolveTokenFk(ctx, entity, transformed, dateEvent, fkPosition, false);
                }
                if (fkPosition == null && fkToken != null) {
                    fkPosition = resolvePositionFromTokenId(ctx, fkToken);
                }
            }
            transformed.put("fkPosition", fkPosition);
            transformed.put("fkTokens", fkToken);
        }
    }

    private LocalDate resolveDateEventWithFallback(Map<String, Object> transformed, String... keys) {
        return toLocalDate(firstNonNull(transformed, keys));
    }

    private void validateRequiredForeignKeys(RunContext ctx, EntityName entity, Map<String, Object> transformed)
            throws MissingForeignKeyException {
        if (entity == null) {
            return;
        }

        switch (entity) {
            case POSITION_TOKENS -> ensureForeignKeyPresent(ctx, entity, transformed, "fkPosition",
                    describePositionDependency(transformed));
            case POSITION_TRANSFERS, EXTRA_INFO -> ensureForeignKeyPresent(ctx, entity, transformed, "fkToken",
                    describeTokenDependency(transformed));
            case EVENTS_WF -> ensureForeignKeyPresent(ctx, entity, transformed, "fkPosition",
                    describePositionDependency(transformed));
            default -> {
                // no FK validation required
            }
        }
    }

    private void ensureForeignKeyPresent(RunContext ctx, EntityName entity, Map<String, Object> transformed,
                                         String fieldName, String dependencyDescription) throws MissingForeignKeyException {
        if (transformed.get(fieldName) != null) {
            return;
        }

        String runId = ctx != null ? ctx.getRunId() : "unknown";
        String message = "Missing required FK " + fieldName
                + " for entity=" + entity.name()
                + " runId=" + runId
                + " " + dependencyDescription;
        warnWithContext(ctx, "FK_MISSING", message);
        throw new MissingForeignKeyException(message);
    }

    private String describePositionDependency(Map<String, Object> transformed) {
        return "dateEvent=" + firstNonNull(transformed, "dateEvent", "DATE_EVENT", "date_event")
                + " nav=" + getStringValueByKeys(transformed, "NAV", "nav")
                + " paEmittente=" + getStringValueByKeys(transformed, "PA_EMITTENTE", "pa_emittente", "paEmittente");
    }

    private String describeTokenDependency(Map<String, Object> transformed) {
        return "dateEvent=" + firstNonNull(transformed, "dateEvent", "DATE_EVENT", "date_event")
                + " fkPosition=" + transformed.get("fkPosition")
                + " iuv=" + getStringValueByKeys(transformed, "IUV", "iuv")
                + " token=" + describeTokenValue(firstNonNull(transformed, "TOKEN", "token"))
                + " tokenPresent=" + (toByteArray(firstNonNull(transformed, "TOKEN", "token")) != null);
    }

    /**
     * Copy anagrafica ID fields from transformed map (where they are Short IDs resolved by resolveAllAnagrafiche).
     * Maps from uppercase ADX column names to camelCase entity field names.
     */
    private void copyAnagraficaFields(Map<String, Object> transformed) {
        // STAZIONE
        Object stazione = transformed.get("STAZIONE");
        if (stazione instanceof Short) {
            transformed.put("stazione", stazione);
        }

        // CANALE
        Object canale = transformed.get("CANALE");
        if (canale instanceof Short) {
            transformed.put("canale", canale);
        }

        // PSP
        Object psp = transformed.get("PSP");
        if (psp instanceof Short) {
            transformed.put("psp", psp);
        }

        // INTERMEDIARIO_PA
        Object intermediarioPa = transformed.get("INTERMEDIARIO_PA");
        if (intermediarioPa instanceof Short) {
            transformed.put("intermediarioPa", intermediarioPa);
        }

        // INTERMEDIARIO_PSP
        Object intermediarioPsp = transformed.get("INTERMEDIARIO_PSP");
        if (intermediarioPsp instanceof Short) {
            transformed.put("intermediarioPsp", intermediarioPsp);
        }

        // FAULT_CODE
        Object faultCode = transformed.get("FAULT_CODE");
        if (faultCode instanceof Short) {
            transformed.put("faultCode", faultCode);
        }

        // TIPO_EVENTO
        Object tipoEvento = transformed.get("TIPO_EVENTO");
        if (tipoEvento instanceof Short) {
            transformed.put("tipoEvento", tipoEvento);
        }
    }

    private Integer resolvePositionFk(RunContext ctx, EntityName entity, Map<String, Object> transformed,
                                     LocalDate dateEvent, LocalDateTime sourceInsertedTs) {
        long startNs = System.nanoTime();
        try {
            String nav = getStringValueByKeys(transformed, "NAV", "nav");
            String paEmittente = getStringValueByKeys(transformed, "PA_EMITTENTE", "pa_emittente", "paEmittente");
            if (nav == null || paEmittente == null) {
                return null;
            }

            BatchLocalCache batchCache = ctx != null ? ctx.getBatchLocalCache() : null;
            Optional<Integer> fkPosition = Optional.empty();

            if (sourceInsertedTs != null) {
                if (batchCache != null) {
                    // Check window-scoped prefetch first (positive hits only; populated before the transform loop)
                    if (batchCache.hasPositionWindowPrefetch(nav, paEmittente, sourceInsertedTs)) {
                        if (ctx != null) {
                            ctx.incrementCacheHitCount();
                        }
                        fkPosition = Optional.of(batchCache.getPositionWindowPrefetch(nav, paEmittente, sourceInsertedTs));
                    } else {
                        Integer cachedWindowId = batchCache.findPositionInWindow(nav, paEmittente, sourceInsertedTs);
                        if (cachedWindowId != null) {
                            if (ctx != null) {
                                ctx.incrementCacheHitCount();
                            }
                            fkPosition = Optional.of(cachedWindowId);
                        } else if (batchCache.hasPositionLookupResult(nav, paEmittente, sourceInsertedTs)) {
                            if (ctx != null) {
                                ctx.incrementCacheHitCount();
                            }
                            fkPosition = Optional.ofNullable(batchCache.getPositionLookupResult(nav, paEmittente, sourceInsertedTs));
                        }
                    }
                }

                if (fkPosition.isEmpty()) {
                    if (ctx != null) {
                        ctx.incrementCacheMissCount();
                        ctx.incrementPositionLookupCount();
                    }
                    LocalDateTime fromInclusive = sourceInsertedTs.minusHours(24);
                    // Partition pruning: constrain DATE_EVENT (the monthly partition key) to the
                    // 1-2 calendar days spanned by the 24h window, so PostgreSQL scans only the
                    // relevant partition(s) instead of every monthly partition.
                    LocalDate dateFrom = fromInclusive.toLocalDate();
                    LocalDate dateTo = sourceInsertedTs.toLocalDate();
                    Integer resolvedId = positionRepository
                            .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                                    nav,
                                    paEmittente,
                                    dateFrom,
                                    dateTo,
                                    fromInclusive,
                                    sourceInsertedTs
                            )
                            .map(it.pagopa.cruscotto.ingestion.entity.Position::getId)
                            .orElse(null);
                    if (batchCache != null) {
                        batchCache.cachePositionLookupResult(nav, paEmittente, sourceInsertedTs, resolvedId);
                    }
                    fkPosition = Optional.ofNullable(resolvedId);
                }
            }

            if (fkPosition.isEmpty() && dateEvent != null) {
                if (batchCache != null && batchCache.hasPositionByDateLookupResult(nav, paEmittente, dateEvent)) {
                    if (ctx != null) {
                        ctx.incrementCacheHitCount();
                    }
                    fkPosition = Optional.ofNullable(batchCache.getPositionByDateLookupResult(nav, paEmittente, dateEvent));
                } else {
                    if (ctx != null) {
                        ctx.incrementCacheMissCount();
                        ctx.incrementPositionLookupCount();
                    }
                    Integer resolvedByDateId = Optional.ofNullable(
                            positionRepository.findLatestIdByBusinessKey(nav, paEmittente, dateEvent)
                    ).orElse(Optional.empty()).orElse(null);
                    if (batchCache != null) {
                        batchCache.cachePositionByDateLookupResult(nav, paEmittente, dateEvent, resolvedByDateId);
                    }
                    fkPosition = Optional.ofNullable(resolvedByDateId);
                }
                if (fkPosition.isPresent()) {
                    Integer resolvedFkPosition = fkPosition.orElseThrow(
                            () -> new IllegalStateException("FK_POSITION unexpectedly absent after fallback lookup"));
                    infoWithContext(ctx, "FK_LOOKUP",
                            "FK_POSITION resolved with date fallback for entity=" + safeEntityName(entity)
                                    + " nav=" + nav
                                    + " paEmittente=" + paEmittente
                                    + " dateEvent=" + dateEvent
                                    + " fkPosition=" + resolvedFkPosition);
                }
            }

            if (fkPosition.isEmpty()) {
                warnWithContext(ctx, "FK_LOOKUP",
                        "FK_POSITION not found for entity=" + safeEntityName(entity)
                                + " nav=" + nav
                                + " paEmittente=" + paEmittente
                                + " dateEvent=" + dateEvent
                                + " sourceInsertedTs=" + sourceInsertedTs);
            }
            return fkPosition.orElse(null);
        } finally {
            if (ctx != null) {
                ctx.addFkPositionDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
            }
        }
    }

    private Integer resolveTokenFkByTokenOnly(RunContext ctx, EntityName entity, Map<String, Object> transformed) {
        long startNs = System.nanoTime();
        try {
            byte[] token = toByteArray(firstNonNull(transformed, "TOKEN", "token"));
            if (token == null) {
                warnWithContext(ctx, "FK_LOOKUP",
                        "FK_TOKEN not found for entity=" + safeEntityName(entity)
                                + " reason=TOKEN_ABSENT");
                return null;
            }

            String tokenBase64 = Base64.getEncoder().encodeToString(token);
            BatchLocalCache batchCache = ctx != null ? ctx.getBatchLocalCache() : null;
            if (batchCache != null) {
                // Check window-scoped prefetch first (positive hits only; populated before the transform loop)
                if (batchCache.hasTokenWindowPrefetch(tokenBase64)) {
                    if (ctx != null) {
                        ctx.incrementCacheHitCount();
                    }
                    return batchCache.getTokenWindowPrefetch(tokenBase64);
                }
                Integer eagerTokenId = batchCache.findToken(tokenBase64);
                if (eagerTokenId != null) {
                    if (ctx != null) {
                        ctx.incrementCacheHitCount();
                    }
                    return eagerTokenId;
                }
                if (batchCache.hasTokenCanonicalLookupResult(tokenBase64)) {
                    if (ctx != null) {
                        ctx.incrementCacheHitCount();
                    }
                    return batchCache.getTokenCanonicalLookupResult(tokenBase64);
                }
            }

            if (ctx != null) {
                ctx.incrementCacheMissCount();
                ctx.incrementTokenLookupCount();
            }
            Integer resolvedByToken = findCanonicalTokenWithPruning(token)
                    .map(it.pagopa.cruscotto.ingestion.entity.PositionTokens::getId)
                    .orElse(null);
            if (batchCache != null) {
                batchCache.cacheTokenCanonicalLookupResult(tokenBase64, resolvedByToken);
            }
            Optional<Integer> byTokenLatest = Optional.ofNullable(resolvedByToken);
            if (byTokenLatest.isPresent()) {
                return byTokenLatest.orElseThrow(
                        () -> new IllegalStateException("FK_TOKEN unexpectedly absent after token lookup"));
            }

            warnWithContext(ctx, "FK_LOOKUP",
                    "FK_TOKEN not found for entity=" + safeEntityName(entity)
                            + " token=" + describeTokenValue(firstNonNull(transformed, "TOKEN", "token"))
                            + " tokenPresent=true");
            return null;
        } finally {
            if (ctx != null) {
                ctx.addFkTokenDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
            }
        }
    }

    private Integer resolveTokenFk(RunContext ctx, EntityName entity, Map<String, Object> transformed,
                                   LocalDate dateEvent, Integer fkPosition, boolean allowPositionIuvFallback) {
        long startNs = System.nanoTime();
        try {
            String iuv = getStringValueByKeys(transformed, "IUV", "iuv");
            byte[] token = toByteArray(firstNonNull(transformed, "TOKEN", "token"));
            BatchLocalCache batchCache = ctx != null ? ctx.getBatchLocalCache() : null;
            if (token != null) {
                String tokenBase64 = Base64.getEncoder().encodeToString(token);
                if (batchCache != null) {
                    Integer eagerTokenId = batchCache.findToken(tokenBase64);
                    if (eagerTokenId != null) {
                        if (ctx != null) {
                            ctx.incrementCacheHitCount();
                        }
                        return eagerTokenId;
                    }
                    if (batchCache.hasTokenCanonicalLookupResult(tokenBase64)) {
                        if (ctx != null) {
                            ctx.incrementCacheHitCount();
                        }
                        Integer cachedCanonicalId = batchCache.getTokenCanonicalLookupResult(tokenBase64);
                        if (cachedCanonicalId != null) {
                            return cachedCanonicalId;
                        }
                    }
                }

                if (ctx != null) {
                    ctx.incrementCacheMissCount();
                    ctx.incrementTokenLookupCount();
                }
                Integer resolvedCanonicalId = findCanonicalTokenWithPruning(token)
                        .map(it.pagopa.cruscotto.ingestion.entity.PositionTokens::getId)
                        .orElse(null);
                if (batchCache != null) {
                    batchCache.cacheTokenCanonicalLookupResult(tokenBase64, resolvedCanonicalId);
                }
                Optional<Integer> byTokenLatest = Optional.ofNullable(resolvedCanonicalId);
                if (byTokenLatest.isPresent()) {
                    return byTokenLatest.orElseThrow(
                            () -> new IllegalStateException("FK_TOKEN unexpectedly absent after token lookup"));
                }

                if (dateEvent != null) {
                    Optional<Integer> byTokenAndDate;
                    if (batchCache != null && batchCache.hasTokenByDateLookupResult(tokenBase64, dateEvent)) {
                        if (ctx != null) {
                            ctx.incrementCacheHitCount();
                        }
                        byTokenAndDate = Optional.ofNullable(batchCache.getTokenByDateLookupResult(tokenBase64, dateEvent));
                    } else {
                        if (ctx != null) {
                            ctx.incrementCacheMissCount();
                            ctx.incrementTokenLookupCount();
                        }
                        Integer resolvedByDateId = Optional.ofNullable(
                                positionTokensRepository.findLatestIdByTokenAndDate(token, dateEvent)
                        ).orElse(Optional.empty()).orElse(null);
                        if (batchCache != null) {
                            batchCache.cacheTokenByDateLookupResult(tokenBase64, dateEvent, resolvedByDateId);
                        }
                        byTokenAndDate = Optional.ofNullable(resolvedByDateId);
                    }
                    if (byTokenAndDate.isPresent()) {
                        return byTokenAndDate.orElseThrow(
                                () -> new IllegalStateException("FK_TOKEN unexpectedly absent after token+date lookup"));
                    }
                }
            }

            if (allowPositionIuvFallback && dateEvent != null && fkPosition != null && iuv != null) {
                    Optional<Integer> byPositionAndIuv;
                    if (batchCache != null && batchCache.hasTokenByPositionIuvLookupResult(fkPosition, iuv, dateEvent)) {
                        if (ctx != null) {
                            ctx.incrementCacheHitCount();
                        }
                        byPositionAndIuv = Optional.ofNullable(
                                batchCache.getTokenByPositionIuvLookupResult(fkPosition, iuv, dateEvent));
                    } else {
                        if (ctx != null) {
                            ctx.incrementCacheMissCount();
                            ctx.incrementTokenLookupCount();
                        }
                        Integer resolvedByPositionIuvId = Optional.ofNullable(
                                positionTokensRepository.findLatestIdByPositionAndIuv(fkPosition, iuv, dateEvent)
                        ).orElse(Optional.empty()).orElse(null);
                        if (batchCache != null) {
                            batchCache.cacheTokenByPositionIuvLookupResult(fkPosition, iuv, dateEvent, resolvedByPositionIuvId);
                        }
                        byPositionAndIuv = Optional.ofNullable(resolvedByPositionIuvId);
                    }
                    if (byPositionAndIuv.isPresent()) {
                        return byPositionAndIuv.orElseThrow(
                                () -> new IllegalStateException("FK_TOKEN unexpectedly absent after position+iuv lookup"));
                    }
            }

            warnWithContext(ctx, "FK_LOOKUP",
                    "FK_TOKEN not found for entity=" + safeEntityName(entity)
                            + " dateEvent=" + dateEvent
                            + " fkPosition=" + fkPosition
                            + " iuv=" + iuv
                            + " token=" + describeTokenValue(firstNonNull(transformed, "TOKEN", "token"))
                            + " tokenPresent=" + (token != null));
            return null;
        } finally {
            if (ctx != null) {
                ctx.addFkTokenDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
            }
        }
    }

    /**
     * Risoluzione della riga TOKEN canonica con partition pruning, delegata a
     * {@link CanonicalTokenResolver} (unico punto in cui vive questa logica, condiviso con
     * EventsWfTransformer e PositionTransfersTransformer).
     */
    private Optional<it.pagopa.cruscotto.ingestion.entity.PositionTokens> findCanonicalTokenWithPruning(byte[] token) {
        return canonicalTokenResolver.findCanonical(token);
    }

    private TokenResolution resolveCanonicalTokenResolution(RunContext ctx, Map<String, Object> transformed) {
        long startNs = System.nanoTime();
        try {
            byte[] token = toByteArray(firstNonNull(transformed, "TOKEN", "token"));
            if (token == null) {
                return TokenResolution.empty();
            }
            String tokenBase64 = Base64.getEncoder().encodeToString(token);
            BatchLocalCache batchCache = ctx != null ? ctx.getBatchLocalCache() : null;
            if (batchCache != null && batchCache.hasTokenCanonicalLookupResult(tokenBase64)) {
                if (ctx != null) {
                    ctx.incrementCacheHitCount();
                }
                Integer cachedTokenId = batchCache.getTokenCanonicalLookupResult(tokenBase64);
                if (cachedTokenId == null) {
                    return TokenResolution.empty();
                }
                if (batchCache.hasTokenCanonicalFkPosition(tokenBase64)) {
                    return new TokenResolution(cachedTokenId, batchCache.getTokenCanonicalFkPosition(tokenBase64));
                }
                return new TokenResolution(cachedTokenId, resolvePositionFromTokenId(ctx, cachedTokenId));
            }

            if (ctx != null) {
                ctx.incrementCacheMissCount();
                ctx.incrementTokenLookupCount();
            }
            Optional<it.pagopa.cruscotto.ingestion.entity.PositionTokens> canonicalToken = findCanonicalTokenWithPruning(token);
            if (batchCache != null && canonicalToken.isEmpty()) {
                batchCache.cacheTokenCanonicalLookupResult(tokenBase64, null);
            }
            if (canonicalToken.isEmpty()) {
                return TokenResolution.empty();
            }
            it.pagopa.cruscotto.ingestion.entity.PositionTokens positionToken = canonicalToken.orElseThrow(
                    () -> new IllegalStateException("Canonical token unexpectedly absent after presence check"));
            if (batchCache != null) {
                batchCache.cacheTokenCanonicalLookupResult(tokenBase64, positionToken.getId());
                batchCache.cacheTokenCanonicalFkPosition(tokenBase64, positionToken.getFkPosition());
            }
            return new TokenResolution(positionToken.getId(), positionToken.getFkPosition());
        } finally {
            if (ctx != null) {
                ctx.addFkTokenDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
            }
        }
    }

    private Integer resolvePositionFromTokenId(RunContext ctx, Integer fkToken) {
        long startNs = System.nanoTime();
        try {
            if (ctx != null) {
                ctx.incrementCacheMissCount();
                ctx.incrementPositionLookupCount();
            }
            return positionTokensRepository.findById(fkToken)
                    .map(it.pagopa.cruscotto.ingestion.entity.PositionTokens::getFkPosition)
                    .orElse(null);
        } finally {
            if (ctx != null) {
                ctx.addFkPositionDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
            }
        }
    }

    private record TokenResolution(Integer fkToken, Integer fkPosition) {
        private static TokenResolution empty() {
            return new TokenResolution(null, null);
        }
    }

    private Integer resolveExistingPositionId(RunContext ctx, String nav, String paEmittente, LocalDateTime insertedTs, BatchLocalCache batchCache) {
        long startNs = System.nanoTime();
        try {
            if (nav == null || paEmittente == null || insertedTs == null) {
                return null;
            }

            // First: check batch local cache (records appena inseriti ma non committati)
            if (batchCache != null) {
                Integer cachedId = batchCache.findPositionInWindow(nav, paEmittente, insertedTs);
                if (cachedId != null) {
                    return cachedId;
                }
                if (batchCache.hasPositionLookupResult(nav, paEmittente, insertedTs)) {
                    return batchCache.getPositionLookupResult(nav, paEmittente, insertedTs);
                }
            }

            // Second: fallback to database query.
            // Partition pruning: constrain DATE_EVENT to the 1-2 calendar days spanned by the
            // 24h window so only the relevant monthly partition(s) are scanned.
            LocalDateTime fromInclusive = insertedTs.minusHours(24);
            Integer resolvedId = positionRepository
                    .findFirstByNavAndPaEmittenteAndDateEventBetweenAndInsertedTimestampBetweenOrderByInsertedTimestampDescIdDesc(
                            nav,
                            paEmittente,
                            fromInclusive.toLocalDate(),
                            insertedTs.toLocalDate(),
                            fromInclusive,
                            insertedTs)
                    .map(it.pagopa.cruscotto.ingestion.entity.Position::getId)
                    .orElse(null);
            if (batchCache != null) {
                batchCache.cachePositionLookupResult(nav, paEmittente, insertedTs, resolvedId);
            }
            return resolvedId;
        } finally {
            if (ctx != null) {
                ctx.addFkPositionDurationMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs));
            }
        }
    }

    private void applyPositionTokenEventRules(Map<String, Object> transformed,
                                              it.pagopa.cruscotto.ingestion.entity.PositionTokens existingToken) {
        String eventType = getStringValueByKeys(transformed, "TIPO_EVENTO_RAW", "TIPO_EVENTO");
        String outcomeReq = getStringValueByKeys(transformed, "OUTCOME_REQ", "outcome_req", "outcomeReq");
        String outcomeResp = getStringValueByKeys(transformed, "OUTCOME_RESP", "outcome_resp", "outcomeResp", "OUTCOME", "outcome");
        String touchpoint = getStringValueByKeys(transformed, "TOUCHPOINT", "touchpoint");
        String paymentMethod = getStringValueByKeys(transformed, "PAYMENT_METHOD", "payment_method", "paymentMethod");
        String faultCode = getStringValueByKeys(transformed, "FAULT_CODE_RAW", "FAULT_CODE", "fault_code");
        LocalDateTime insertedTimestampReq = toLocalDateTime(firstNonNull(transformed,
                "INSERTED_TIMESTAMP_REQ", "inserted_timestamp_req", "insertedTimestampReq"));

        LocalDateTime currentPaymentDate = toLocalDateTime(firstNonNull(transformed, "paymentDate", "PAYMENT_DATE", "payment_date"));
        if (currentPaymentDate == null && existingToken != null) {
            currentPaymentDate = existingToken.getPaymentDate();
        }

        String currentOutcome = getStringValueByKeys(transformed, "outcome", "OUTCOME", "outcomeResp");
        if (currentOutcome == null && existingToken != null) {
            currentOutcome = existingToken.getOutcome();
        }

        if ("sendPaymentOutcome".equals(eventType) || "sendPaymentOutcomeV2".equals(eventType)) {
            if ("OK".equals(outcomeResp)) {
                transformed.put("outcome", outcomeReq);
                if ("OK".equals(outcomeReq) && currentPaymentDate == null && insertedTimestampReq != null) {
                    transformed.put("paymentDate", insertedTimestampReq);
                    if ("Touchpoint PSP".equals(touchpoint)) {
                        transformed.put("paymentMethod", paymentMethod);
                    }
                }
            } else if ("KO".equals(outcomeResp)
                    && isTokenScaduto(faultCode)
                    && "Touchpoint PSP".equals(touchpoint)) {
                transformed.put("outcome", outcomeReq);
                if ("OK".equals(outcomeReq) && currentPaymentDate == null && insertedTimestampReq != null) {
                    transformed.put("paymentDate", insertedTimestampReq);
                }
                transformed.put("paymentMethod", paymentMethod);
            }
        }

        if (("activatePaymentNotice".equals(eventType) || "activatePaymentNoticeV2".equals(eventType))
                && "OK".equals(outcomeResp)) {
            String creditorRefId = getStringValueByKeys(transformed, "CREDITOR_REF_ID", "creditor_ref_id", "creditorRefId");
            String iuv = getStringValueByKeys(transformed, "IUV", "iuv");
            if (creditorRefId != null && creditorRefId.equals(iuv)) {
                transformed.put("creditorRefId", null);
            }
        }

        if ("pspNotifyPayment".equals(eventType) || "pspNotifyPaymentV2".equals(eventType)) {
            if ("KO".equals(outcomeResp) && isBlank(currentOutcome)) {
                transformed.put("outcome", "KO");
            }
            // TODO: sync PSP/INTERMEDIARIO_PSP/CANALE to POSITION_TRANSFERS when columns are available in schema.
        }

        if ("closePayment".equals(eventType) || "closePayment-v2".equals(eventType)) {
            if ("KO".equals(outcomeReq) && "OK".equals(outcomeResp) && isBlank(currentOutcome)) {
                transformed.put("outcome", "KO");
            }
        }
    }

    private void mergeTokenWithExistingState(Map<String, Object> transformed,
                                             it.pagopa.cruscotto.ingestion.entity.PositionTokens existingToken) {
        if (existingToken == null) {
            return;
        }

        mergeIfMissing(transformed, "dateEvent", existingToken.getDateEvent());
        mergeIfMissing(transformed, "fkPosition", existingToken.getFkPosition());
        mergeIfMissing(transformed, "amount", existingToken.getAmount());
        mergeIfMissing(transformed, "fee", existingToken.getFee());
        mergeIfMissing(transformed, "iuv", existingToken.getIuv());
        mergeIfMissing(transformed, "creditorRefId", existingToken.getCreditorRefId());
        mergeIfMissing(transformed, "outcome", existingToken.getOutcome());
        mergeIfMissing(transformed, "idCarrello", existingToken.getIdCarrello());
        mergeIfMissing(transformed, "stazione", existingToken.getStazione());
        mergeIfMissing(transformed, "canale", existingToken.getCanale());
        mergeIfMissing(transformed, "intermediarioPa", existingToken.getIntermediarioPa());
        mergeIfMissing(transformed, "intermediarioPsp", existingToken.getIntermediarioPsp());
        mergeIfMissing(transformed, "psp", existingToken.getPsp());
        mergeIfMissing(transformed, "touchpoint", existingToken.getTouchpoint());
        mergeIfMissing(transformed, "paymentMethod", existingToken.getPaymentMethod());
        mergeIfMissing(transformed, "paymentDate", existingToken.getPaymentDate());
    }

    private void mergeIfMissing(Map<String, Object> transformed, String key, Object fallbackValue) {
        if (fallbackValue == null) {
            return;
        }
        Object current = transformed.get(key);
        if (current == null) {
            transformed.put(key, fallbackValue);
            return;
        }
        if (current instanceof String str && str.isBlank()) {
            transformed.put(key, fallbackValue);
        }
    }

    private boolean isTokenScaduto(String faultCode) {
        return "PPT_TOKEN_SCADUTO".equals(faultCode) || "PPT_TOKEN_SCADUTO_KO".equals(faultCode);
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private void infoWithContext(RunContext ctx, String phase, String message) {
        if (ctx != null) {
            LogHelper.info(ctx, phase, message);
            return;
        }
        log.info(message);
    }

    private void warnWithContext(RunContext ctx, String phase, String message) {
        if (ctx != null) {
            if (shouldSampleEventsWfLookupNoise(ctx, phase, message)) {
                LogHelper.warn(ctx, phase, message);
                return;
            }
            if (isEventsWfLookupNoise(ctx, phase)) {
                LogHelper.debug(ctx, phase, message);
                return;
            }
            LogHelper.warn(ctx, phase, message);
            return;
        }
        log.warn(message);
    }

    private boolean shouldSampleEventsWfLookupNoise(RunContext ctx, String phase, String message) {
        if (!isEventsWfLookupNoise(ctx, phase)) {
            return false;
        }
        return Math.floorMod(message.hashCode(), 100) == 0;
    }

    private boolean isEventsWfLookupNoise(RunContext ctx, String phase) {
        return ctx != null
                && EntityName.EVENTS_WF.name().equals(ctx.getEntityName())
                && ("FK_LOOKUP".equals(phase) || "FK_MISSING".equals(phase));
    }

    private String safeEntityName(EntityName entity) {
        return entity != null ? entity.name() : "unknown";
    }

    private String describeTokenValue(Object tokenValue) {
        if (tokenValue == null) {
            return "null";
        }
        String rendered;
        if (tokenValue instanceof byte[] bytes) {
            rendered = new String(bytes, StandardCharsets.UTF_8);
        } else {
            rendered = String.valueOf(tokenValue);
        }
        String normalized = rendered.replaceAll("\\s+", " ").trim();
        if (normalized.isEmpty()) {
            return "blank";
        }
        return normalized.length() > 128 ? normalized.substring(0, 128) + "..." : normalized;
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
            if (value instanceof CharSequence seq && seq.toString().trim().isEmpty()) {
                continue;
            }
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String getStringValueByKeys(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            String value = getStringValue(map, key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    /**
     * Cast numerici espliciti per AMOUNT, FEE, etc.
     */
    private void castNumericFields(Map<String, Object> transformed) {
        castToBigDecimal(transformed, "AMOUNT");
        castToBigDecimal(transformed, "FEE");
        castToBigDecimal(transformed, "TRANSFER_AMOUNT");
        castToBigDecimal(transformed, "AMOUNT_TRANSFER");
    }

    /**
     * Cast a BigDecimal con fallback a null.
     */
    private void castToBigDecimal(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return;
        try {
            if (value instanceof BigDecimal bd) {
                // già BigDecimal
                return;
            }
            if (value instanceof Number num) {
                map.put(key, BigDecimal.valueOf(num.doubleValue()));
            } else if (value instanceof String str) {
                map.put(key, new BigDecimal(str));
            }
        } catch (Exception e) {
            log.warn("Failed to cast {} to BigDecimal: {} (type={})", key, value, value.getClass().getSimpleName());
            map.put(key, null);
        }
    }

    /**
     * Estrai valore stringa da mappa, supportando vari tipi.
     */
    private String getStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof String s) return s.isBlank() ? null : s;
        return value.toString();
    }

    /**
     * Converti Object a LocalDate.
     */
    private LocalDate toLocalDate(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof LocalDate ld) return ld;
            if (value instanceof LocalDateTime ldt) return ldt.toLocalDate();
            if (value instanceof OffsetDateTime odt) return odt.toLocalDate();
            if (value instanceof ZonedDateTime zdt) return zdt.toLocalDate();
            if (value instanceof Instant inst) return inst.atZone(ZoneOffset.UTC).toLocalDate();
            if (value instanceof java.util.Date date) return date.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
            if (value instanceof Number epoch) {
                return Instant.ofEpochMilli(epoch.longValue()).atZone(ZoneOffset.UTC).toLocalDate();
            }
            if (value instanceof Long epoch) return new java.util.Date(epoch).toInstant()
                    .atZone(ZoneOffset.UTC).toLocalDate();
            if (value instanceof String str && !str.isBlank()) {
                String normalized = str.trim().replace(' ', 'T');
                try {
                    return LocalDate.parse(normalized);
                } catch (Exception ignored) {
                    try {
                        return OffsetDateTime.parse(normalized).toLocalDate();
                    } catch (Exception ignoredAgain) {
                        try {
                            return LocalDateTime.parse(normalized).toLocalDate();
                        } catch (Exception ignoredThird) {
                            return Instant.parse(normalized).atZone(ZoneOffset.UTC).toLocalDate();
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to convert to LocalDate: {}", value);
        }
        return null;
    }

    private LocalDateTime toLocalDateTime(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof LocalDateTime ldt) return ldt;
            if (value instanceof LocalDate ld) return ld.atStartOfDay();
            if (value instanceof Instant inst) return LocalDateTime.ofInstant(inst, ZoneOffset.UTC);
            if (value instanceof java.util.Date date) return LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC);
            if (value instanceof Number epochMillis) {
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis.longValue()), ZoneOffset.UTC);
            }
            if (value instanceof String str && !str.isBlank()) {
                try {
                    return LocalDateTime.parse(str);
                } catch (Exception ignored) {
                    return LocalDateTime.ofInstant(Instant.parse(str), ZoneOffset.UTC);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to convert to LocalDateTime: {}", value);
        }
        return null;
    }

    private Short toShort(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof Short s) return s;
            if (value instanceof Number n) return n.shortValue();
            if (value instanceof String s && !s.isBlank()) return Short.parseShort(s.trim());
        } catch (Exception e) {
            log.warn("Failed to convert to Short: {}", value);
        }
        return null;
    }

    private Boolean toBoolean(Object value) {
        if (value == null) return null;
        if (value instanceof Boolean b) return b;
        if (value instanceof Number n) return n.intValue() != 0;
        if (value instanceof String s) {
            String normalized = s.trim().toLowerCase();
            if (normalized.equals("true") || normalized.equals("1") || normalized.equals("yes") || normalized.equals("y")) {
                return true;
            }
            if (normalized.equals("false") || normalized.equals("0") || normalized.equals("no") || normalized.equals("n")) {
                return false;
            }
        }
        return null;
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value == null) return null;
        try {
            if (value instanceof BigDecimal bd) return bd;
            if (value instanceof Number num) return BigDecimal.valueOf(num.doubleValue());
            if (value instanceof String str && !str.isBlank()) return new BigDecimal(str.trim());
        } catch (Exception e) {
            log.warn("Failed to convert to BigDecimal: {}", value);
        }
        return null;
    }

    private byte[] toByteArray(Object value) {
        if (value == null) return null;
        if (value instanceof byte[] bytes) return bytes;
        if (value instanceof String str) {
            if (str.isBlank()) return null;
            return str.getBytes(StandardCharsets.UTF_8);
        }
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

}
