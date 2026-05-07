package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * KustoQL query builder for EXTRA_INFO entity.
 * Finestra consigliata: 30 minuti.
 *
 * Query template loaded from: classpath:queries/adx/extra_info.kql
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExtraInfoAdxQueryBuilder implements AdxEntityQueryBuilder {
    private final QueryTemplateLoader templateLoader;
    private final IngestionConfigProvider configProvider;

    @Override
    public String buildQuery(RunContext ctx, Instant fromInclusive, Instant toExclusive) {
        log.debug("ADX_QUERY_TEMPLATE runId={} entityName=EXTRA_INFO from={} to={}",
                ctx.getRunId(), fromInclusive, toExclusive);

        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("start", toKustoDateTime(fromInclusive));
        placeholders.put("end", toKustoDateTime(toExclusive));
        placeholders.put("estimates", buildEstimatesClause());

        return templateLoader.loadAndSubstitute("extra_info", placeholders);
    }

    private String buildEstimatesClause() {
        if (configProvider.isIncludeEstimates()) {
            return "| extend DimensioneRiga = estimate_data_size(*)\n"
                    + "| summarize NUMERI_RIGHE=count(), SIZE_IN_MB = sum(DimensioneRiga) / 1024 / 1024\n";
        }
        return "";
    }

    private String toKustoDateTime(Instant instant) {
        return instant.toString();
    }
}


