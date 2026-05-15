package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * KustoQL query builder for POSITION_TRANSFERS entity.
 * Finestra consigliata: 30 minuti.
 *
 * Query template loaded from: classpath:queries/adx/transfers.kql
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TransfersAdxQueryBuilder implements AdxEntityQueryBuilder {
    private final QueryTemplateLoader templateLoader;
    private final IngestionConfigProvider configProvider;
    private final AdxTableNamesConfig tableNamesConfig;

    @Override
    public String buildQuery(RunContext ctx, Instant fromInclusive, Instant toExclusive) {
        log.debug("ADX_QUERY_TEMPLATE runId={} entityName=POSITION_TRANSFERS from={} to={}",
                ctx.getRunId(), fromInclusive, toExclusive);

        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("start", toKustoDateTime(fromInclusive));
        placeholders.put("end", toKustoDateTime(toExclusive));
        placeholders.put("table_name", tableNamesConfig.getTableName("POSITION_TRANSFERS"));
        placeholders.put("estimates", buildEstimatesClause());

        return templateLoader.loadAndSubstitute("transfers", placeholders);
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


