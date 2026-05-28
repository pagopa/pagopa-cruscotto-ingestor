package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.ExtraInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

/**
 * Trasformer specializzato per EXTRA_INFO.
 * Regole:
 * - INSERT idempotente.
 * - FK_TOKEN obbligatoria.
 * - Nessun update.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExtraInfoTransformer {

    private final EntityTransformerImpl baseTransformer;

    /**
     * Trasformare un EXTRA_INFO.
     */
    public ExtraInfo transform(Map<String, Object> row, RunContext ctx) throws EntityTransformer.TransformationException {
        String runId = ctx.getRunId();

        try {
            Map<String, Object> transformed = new java.util.HashMap<>(row);
            baseTransformer.resolveAllAnagrafiche(runId, transformed);

            ExtraInfo extraInfo = new ExtraInfo();

            // DATE_EVENT
            Instant insertedTs = toInstant(transformed.get("INSERTED_TIMESTAMP"));
            if (insertedTs != null) {
                extraInfo.setDateEvent(insertedTs.atZone(ZoneOffset.UTC).toLocalDate());
            }

            // Campi base
            extraInfo.setInfoName((String) transformed.get("INFO_NAME"));
            extraInfo.setInfoValue((String) transformed.get("INFO_VALUE"));

            // Anagrafica
            extraInfo.setTipoEvento((Short) transformed.get("TIPO_EVENTO"));

            // FK_TOKEN: TODO da risolvere dal context

            return extraInfo;
        } catch (Exception e) {
            log.error("[{}] [TRANSFORM] Failed to transform EXTRA_INFO: {}", runId, e.getMessage());
            throw new EntityTransformer.TransformationException("EXTRA_INFO transform failed", e);
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
}

