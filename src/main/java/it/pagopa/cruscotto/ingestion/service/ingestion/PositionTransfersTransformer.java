package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.PositionTransfers;
import it.pagopa.cruscotto.ingestion.repository.PositionTokensRepository;
import it.pagopa.cruscotto.ingestion.repository.PositionTransfersRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

/**
 * Trasformer specializzato per POSITION_TRANSFERS.
 * Regole:
 * - Ogni TRANSFER associato a un TOKEN.
 * - AMOUNT_TRANSFER: cast numerico di TRANSFER_AMOUNT.
 * - FK_TOKEN risolto via TOKEN.
 * - Regola 7.4: Idempotente su rilettura.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PositionTransfersTransformer {

    private final EntityTransformerImpl baseTransformer;
    private final PositionTokensRepository positionTokensRepository;
    private final PositionTransfersRepository positionTransfersRepository;

    /**
     * Trasformare un POSITION_TRANSFER.
     * Regola 7.4:
     * - Recuperare l'ID del TOKEN associato tramite chiave TOKEN.
     * - INSERT del TRANSFER.
     * - In caso di rilettura dello stesso TRANSFER → operazione idempotente.
     */
    public PositionTransfers transform(Map<String, Object> row, RunContext ctx) throws EntityTransformer.TransformationException {
        String runId = ctx.getRunId();

        try {
            Map<String, Object> transformed = new java.util.HashMap<>(row);
            baseTransformer.resolveAllAnagrafiche(runId, transformed);

            PositionTransfers transfer = new PositionTransfers();

            // DATE_EVENT da INSERTED_TIMESTAMP
            Instant insertedTs = toInstant(transformed.get("INSERTED_TIMESTAMP"));
            if (insertedTs != null) {
                transfer.setDateEvent(insertedTs.atZone(ZoneOffset.UTC).toLocalDate());
            }

            // Campi da ADX
            transfer.setPaTransfer((String) transformed.get("PA_TRANSFER"));
            transfer.setIdTransfer((Short) transformed.get("ID_TRANSFER"));
            transfer.setIbanTransfer((String) transformed.get("IBAN_TRANSFER"));
            transfer.setAmountTransfer(toBigDecimal(transformed.get("TRANSFER_AMOUNT")));
            Object isBolloObj = transformed.get("IS_BOLLO");
            if (isBolloObj != null) {
                transfer.setIsBollo(Boolean.parseBoolean(isBolloObj.toString()));
            }

            // Implementare Regola 7.4: Risolvere FK_TOKEN via TOKEN
            // E verificare se TRANSFER già esiste (idempotenza)
            byte[] tokenBytes = toBytes(transformed.get("TOKEN"));
            if (tokenBytes != null) {
                Optional<Integer> fkTokenOpt = positionTokensRepository.findLatestByToken(tokenBytes)
                        .map(pt -> pt.getId());

                if (fkTokenOpt.isPresent()) {
                    Integer fkToken = fkTokenOpt.orElseThrow(
                            () -> new IllegalStateException("FK_TOKEN unexpectedly absent"));
                    transfer.setFkToken(fkToken);
                    log.debug("[{}] [TRANSFORM] POSITION_TRANSFERS FK_TOKEN resolved: fkToken={}",
                            runId, fkToken);

                    // Verificare idempotenza: se TRANSFER già esiste, impostare ID per UPDATE
                    String paTransfer = transfer.getPaTransfer();
                    Short idTransfer = transfer.getIdTransfer();
                    Optional<PositionTransfers> existingTransferOpt =
                            positionTransfersRepository.findLatestByTokenAndTransferId(
                                    fkToken, paTransfer, idTransfer);

                    if (existingTransferOpt.isPresent()) {
                        PositionTransfers existing = existingTransferOpt.orElseThrow(
                                () -> new IllegalStateException("Existing TRANSFER unexpectedly absent"));
                        transfer.setId(existing.getId());
                        log.debug("[{}] [TRANSFORM] POSITION_TRANSFERS UPDATE (idempotent): id={} fkToken={} paTransfer={}",
                                runId, existing.getId(), fkToken, paTransfer);
                    } else {
                        log.debug("[{}] [TRANSFORM] POSITION_TRANSFERS INSERT: new transfer fkToken={} paTransfer={}",
                                runId, fkToken, paTransfer);
                    }
                } else {
                    log.warn("[{}] [TRANSFORM] POSITION_TRANSFERS FK_TOKEN NOT FOUND for token",
                            runId);
                }
            } else {
                log.warn("[{}] [TRANSFORM] POSITION_TRANSFERS TOKEN is empty/null", runId);
            }

            return transfer;
        } catch (Exception e) {
            log.error("[{}] [TRANSFORM] Failed to transform POSITION_TRANSFERS: {}", runId, e.getMessage(), e);
            throw new EntityTransformer.TransformationException("POSITION_TRANSFERS transform failed", e);
        }
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
}

