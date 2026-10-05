package it.pagopa.cruscotto.ingestion.batch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.entity.EventsWf;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
import it.pagopa.cruscotto.ingestion.entity.Position;
import it.pagopa.cruscotto.ingestion.entity.PositionTokens;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.entity.StagingIngestError;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.service.ExtraInfoWhitelistService;
import it.pagopa.cruscotto.ingestion.service.StagingErrorService;
import it.pagopa.cruscotto.ingestion.service.ingestion.BulkWriter;
import it.pagopa.cruscotto.ingestion.service.ingestion.EntityTransformer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobParameters;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReconciliationIngestionRunner {

    private final StagingErrorService stagingErrorService;
    private final EntityTransformer entityTransformer;
    private final BulkWriter bulkWriter;
    private final ObjectMapper objectMapper;
    private final IngestionConfig ingestionConfig;
    private final ExtraInfoWhitelistService extraInfoWhitelistService;

    public void run(JobParameters jobParameters) {
        String runId = jobParameters.getString(JobParameterKeys.RUN_ID);
        int maxRetries = ingestionConfig.getStaging().getMaxRetries();

        if (!ingestionConfig.getReconciliation().isEnabled()) {
            log.info("[runId={}][phase=NOOP] Reconciliation disabled via config", runId);
            return;
        }

        // ── Auto-unpark: riporta in PENDING i record PARKED abbastanza vecchi ──────────
        // Garantisce che nessuna riga ADX venga persa definitivamente: quando l'entità
        // padre ha raggiunto il timestamp mancante, il record riceve un nuovo ciclo di retry.
        Duration unparkAfter = ingestionConfig.getStaging().getUnparkAfter();
        int batchSize = ingestionConfig.getReconciliation().getBatchSize();
        // Bound inferiore su CREATED_AT: STG_INGEST_ERROR e' partizionata per giorno con 730
        // partizioni pre-create, quindi ogni query senza questo predicato le aprirebbe tutte. Coincide
        // con la retention, oltre la quale le partizioni vengono svuotate: i record esclusi sono
        // quelli che stanno per sparire comunque.
        OffsetDateTime createdAtFrom = retentionFloor();
        if (unparkAfter != null) {
            int unparked = stagingErrorService.unparkOldRecords(unparkAfter, batchSize, createdAtFrom);
            if (unparked > 0) {
                log.info("[runId={}][phase=UNPARK] records={} unparkAfter={}", runId, unparked, unparkAfter);
            }
        }

        // ── Drain con budget di tempo ────────────────────────────────────────────────────
        // Prima si processava UN solo batch per entita' per esecuzione: con batch-size 100 e cadenza
        // oraria il tetto era ~500 record/ora, mentre un singolo run di ingestion puo' accodarne
        // centinaia di migliaia. La coda non era smaltibile e i record morivano per retention invece
        // di essere recuperati. Ora si cicla finche' c'e' lavoro, fermandosi al budget: la capacita'
        // si adatta all'arretrato invece di essere un numero fisso.
        long startedNanos = System.nanoTime();
        Duration maxDuration = ingestionConfig.getReconciliation().getMaxDuration();
        long backlogBefore = stagingErrorService.countPending(createdAtFrom);
        long processed = 0;
        boolean moreWork = true;
        while (moreWork && !isOverBudget(runId, startedNanos, maxDuration, processed)) {
            moreWork = false;
            for (EntityName entity : EntityName.values()) {
                if (!hasPendingRecords(entity)) {
                    continue;
                }

                List<StagingIngestError> pending = stagingErrorService.fetchPending(entity, batchSize, createdAtFrom);
                if (pending.isEmpty()) {
                    continue;
                }
                // Un batch pieno significa che con ogni probabilita' c'e' altro dietro: si rifa' un giro.
                moreWork |= pending.size() == batchSize;
                processed += pending.size();

                log.info("[runId={}][entity={}][phase=START] pendingCount={}", runId, entity.name(), pending.size());

                for (StagingIngestError record : pending) {
                    // Verificare se il record ha superato il limite di retry
                    int currentRetryCount = record.getRetryCount() != null ? record.getRetryCount() : 0;
                    if (currentRetryCount >= maxRetries) {
                        stagingErrorService.markParked(record, runId,
                                new IllegalStateException("Record parked after exhausting reconciliation retries"),
                                currentRetryCount);
                        log.error("[runId={}][entity={}][phase=ERROR] stagingId={} retryCount={} maxRetries={} marked=PARKED",
                                runId, entity.name(), record.getId(), currentRetryCount, maxRetries);
                        continue;
                    }

                    try {
                        Map<String, Object> payload = objectMapper.readValue(
                                record.getPayloadJson(),
                                new TypeReference<>() {}
                        );

                        RunContext ctx = new RunContext(entity.name(), runId, Instant.now());
                        ctx.setOperationId(record.getOperationId());

                        List<Map<String, Object>> normalizedPayloads = expandPayloadForReconciliation(entity, payload);
                        if (normalizedPayloads.isEmpty()) {
                            stagingErrorService.markDone(record, runId);
                            log.info("[runId={}][entity={}][phase=NOOP] stagingId={} skipped by EXTRA_INFO whitelist marked=DONE",
                                    runId, entity.name(), record.getId());
                            continue;
                        }

                        List<Object> transformedBatch = new ArrayList<>(normalizedPayloads.size());
                        for (Map<String, Object> normalizedPayload : normalizedPayloads) {
                            transformedBatch.add(entityTransformer.transform(normalizedPayload, getTargetClass(entity), ctx, entity));
                        }

                        // Errori di trasformazione (dominio) → staging via exception handler
                        // Successo bulk → DONE
                        bulkWriter.writeBulk(entity, transformedBatch, runId, ctx.getBatchLocalCache());
                        stagingErrorService.markDone(record, runId);

                        log.info("[runId={}][entity={}][phase=BULK_OK] stagingId={} recordsWritten={} marked=DONE",
                                runId, entity.name(), record.getId(), transformedBatch.size());

                    } catch (BulkWriter.BulkWriteException e) {
                        int nextRetryCount = currentRetryCount + 1;
                        if (nextRetryCount >= maxRetries) {
                            stagingErrorService.markParked(record, runId, e, nextRetryCount);
                            log.error("[runId={}][entity={}][phase=BULK_KO_TOTAL] stagingId={} retryCount={} maxRetries={} marked=PARKED error={}",
                                    runId, entity.name(), record.getId(), nextRetryCount, maxRetries, e.getMessage());
                        } else {
                            stagingErrorService.markRetryFailed(record, runId, e);
                            log.error("[runId={}][entity={}][phase=BULK_KO_TOTAL] stagingId={} retryCount={} error={}",
                                    runId, entity.name(), record.getId(), nextRetryCount, e.getMessage());
                        }

                    } catch (Exception ex) {
                        int nextRetryCount = currentRetryCount + 1;
                        if (nextRetryCount >= maxRetries) {
                            stagingErrorService.markParked(record, runId, ex, nextRetryCount);
                            log.error("[runId={}][entity={}][phase=ERROR] stagingId={} sourceKey={} retryCount={} maxRetries={} marked=PARKED message={}",
                                    runId, entity.name(), record.getId(), record.getSourceKey(), nextRetryCount, maxRetries, ex.getMessage());
                        } else {
                            stagingErrorService.markRetryFailed(record, runId, ex);
                            log.error("[runId={}][entity={}][phase=ERROR] stagingId={} sourceKey={} retryCount={} message={}",
                                    runId, entity.name(), record.getId(), record.getSourceKey(), nextRetryCount, ex.getMessage());
                        }
                    }
                }

                log.info("[runId={}][entity={}][phase=END] processedCount={}", runId, entity.name(), pending.size());
            }
        }

        long backlogAfter = stagingErrorService.countPending(createdAtFrom);
        // L'arretrato prima e dopo e' l'unico numero che dice se la coda si sta smaltendo o crescendo:
        // senza, un accumulo si nota solo quando lo segnala il cliente.
        log.info("[runId={}][phase=DRAIN_END] processed={} backlogBefore={} backlogAfter={} elapsedMs={} drained={}",
                runId, processed, backlogBefore, backlogAfter, elapsedMs(startedNanos), !moreWork);
        if (backlogAfter > backlogBefore) {
            log.warn("[runId={}][phase=BACKLOG_GROWING] backlogBefore={} backlogAfter={} — lo staging cresce "
                            + "piu' di quanto la riconciliazione riesca a smaltire: i record piu' vecchi verranno "
                            + "eliminati dalla retention senza essere recuperati",
                    runId, backlogBefore, backlogAfter);
        }
    }

    /**
     * Tetto di durata complessiva del drain: il job non deve sovrapporsi alla propria esecuzione
     * successiva ne' competere indefinitamente con l'ingestion per le connessioni del pool. Il
     * progresso non va perso: ogni record e' chiuso nella propria transazione.
     */
    private boolean isOverBudget(String runId, long startedNanos, Duration maxDuration, long processed) {
        if (maxDuration == null || maxDuration.isZero() || maxDuration.isNegative()) {
            return false;
        }
        long elapsedMs = elapsedMs(startedNanos);
        if (elapsedMs < maxDuration.toMillis()) {
            return false;
        }
        log.warn("[runId={}][phase=BUDGET_EXCEEDED] maxDuration={} elapsedMs={} processed={} — resta "
                        + "dell'arretrato, riprende alla prossima esecuzione",
                runId, maxDuration, elapsedMs, processed);
        return true;
    }

    private static long elapsedMs(long startedNanos) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }

    /**
     * Soglia inferiore su {@code CREATED_AT} per le query sullo staging.
     *
     * <p>Serve al partition pruning: la tabella e' partizionata per giorno con 730 partizioni
     * pre-create. Coincide con la retention del cleanup, perche' oltre quella soglia le partizioni
     * vengono svuotate: ritentare quei record sarebbe banda sottratta a quelli ancora recuperabili.
     * Un giorno di margine copre lo sfasamento fra la soglia e l'esecuzione notturna del cleanup.</p>
     */
    private OffsetDateTime retentionFloor() {
        Duration retention = ingestionConfig.getStagingErrorCleanup() != null
                ? ingestionConfig.getStagingErrorCleanup().getRetention()
                : null;
        Duration window = retention != null ? retention.plusDays(1) : Duration.ofDays(8);
        return OffsetDateTime.now(ZoneOffset.UTC).minus(window);
    }

    private boolean hasPendingRecords(EntityName entity) {
        try {
            // Filtrare solo le entità gestite; le entità senza classe target vengono saltate silenziosamente
            getTargetClass(entity);
            return true;
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private Class<?> getTargetClass(EntityName entityName) {
        return switch (entityName) {
            case POSITION -> Position.class;
            case POSITION_TOKENS -> PositionTokens.class;
            case POSITION_TRANSFERS -> PositionTransfers.class;
            case EXTRA_INFO -> ExtraInfo.class;
            case EVENTS_WF -> EventsWf.class;
            default -> throw new IllegalArgumentException("No reconciliation target class configured for entity: " + entityName);
        };
    }

    private List<Map<String, Object>> expandPayloadForReconciliation(EntityName entity, Map<String, Object> payload) {
        if (entity != EntityName.EXTRA_INFO) {
            return List.of(payload);
        }

        String infoName = getStringValueByKeys(payload, "INFO_NAME", "info_name", "infoName");
        if (infoName != null) {
            if (isExtraInfoInfoNameNotAllowed(infoName)) {
                return List.of();
            }
            return List.of(payload);
        }

        Object additionalInfo = firstNonNull(payload, "ADDITIONAL_INFO", "additional_info", "additionalInfo");
        if (additionalInfo == null) {
            return List.of(payload);
        }

        Map<String, Object> additionalInfoMap = parseAdditionalInfo(additionalInfo);
        if (additionalInfoMap.isEmpty()) {
            return List.of(payload);
        }

        List<Map<String, Object>> expanded = new ArrayList<>(additionalInfoMap.size());
        for (Map.Entry<String, Object> entry : additionalInfoMap.entrySet()) {
            if (isExtraInfoInfoNameNotAllowed(entry.getKey())) {
                continue;
            }
            Map<String, Object> propertyPayload = new LinkedHashMap<>(payload);
            propertyPayload.put("INFO_NAME", entry.getKey());
            propertyPayload.put("INFO_VALUE", stringifyInfoValue(entry.getValue()));
            expanded.add(propertyPayload);
        }
        return expanded;
    }

    private String stringifyInfoValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception ignored) {
            return String.valueOf(value);
        }
    }

    private Map<String, Object> parseAdditionalInfo(Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            Map<String, Object> normalized = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                normalized.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            return normalized;
        }

        if (value instanceof String textValue) {
            String normalized = textValue.trim();
            if (normalized.isEmpty()) {
                return Map.of();
            }
            try {
                Map<String, Object> parsed = objectMapper.readValue(normalized, new TypeReference<>() {});
                return parsed != null ? parsed : Map.of();
            } catch (Exception ignored) {
                return Map.of();
            }
        }

        try {
            Map<String, Object> converted = objectMapper.convertValue(value, new TypeReference<>() {});
            return converted != null ? converted : Map.of();
        } catch (IllegalArgumentException ignored) {
            return Map.of();
        }
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

    private String getStringValueByKeys(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            Object value = map.get(key);
            if (value == null) {
                continue;
            }
            String rendered = String.valueOf(value).trim();
            if (!rendered.isEmpty()) {
                return rendered;
            }
        }
        return null;
    }

    private boolean isExtraInfoInfoNameNotAllowed(String infoName) {
        return !extraInfoWhitelistService.isAllowed(infoName);
    }
}






