package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * KustoQL query builder for EVENTS_WF entity.
 * Finestra consigliata: 10 minuti; la RESP usa end_resp = end + 5 minuti.
 *
 * NOTE: This builder generates TWO separate queries:
 * 1. req/resp join (!paSendRT/paSendRTV2) - loaded from: classpath:queries/adx/events_wf_req_resp.kql
 * 2. receipt (paSendRT/paSendRTV2) - loaded from: classpath:queries/adx/events_wf_receipt.kql
 *
 * The AdxQueryService will execute both queries.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventsWfAdxQueryBuilder implements AdxEntityQueryBuilder {
    private final QueryTemplateLoader templateLoader;
    private final IngestionConfigProvider configProvider;
    private final AdxTableNamesConfig tableNamesConfig;

    private static final Duration RESP_ADDITIONAL_WINDOW = Duration.ofMinutes(5);

    /**
     * Builds the REQ/RESP join query for EVENTS_WF (excluding paSendRT/paSendRTV2).
     */
    public String buildReqRespQuery(RunContext ctx, Instant fromInclusive, Instant toExclusive) {
        log.debug("ADX_QUERY_TEMPLATE runId={} entityName=EVENTS_WF type=REQ_RESP from={} to={}",
                ctx.getRunId(), fromInclusive, toExclusive);

        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("start", toKustoDateTime(fromInclusive));
        placeholders.put("end", toKustoDateTime(toExclusive));
        placeholders.put("table_name", tableNamesConfig.getTableName("EVENTS_WF"));
        placeholders.put("estimates", buildEstimatesClause());

        return templateLoader.loadAndSubstitute("events_wf_req_resp", placeholders);
    }

    /**
     * Builds the receipt query for EVENTS_WF (paSendRT/paSendRTV2).
     */
    public String buildReceiptQuery(RunContext ctx, Instant fromInclusive, Instant toExclusive) {
        log.debug("ADX_QUERY_TEMPLATE runId={} entityName=EVENTS_WF type=RECEIPT from={} to={}",
                ctx.getRunId(), fromInclusive, toExclusive);

        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("start", toKustoDateTime(fromInclusive));
        placeholders.put("end", toKustoDateTime(toExclusive));
        placeholders.put("table_name", tableNamesConfig.getTableName("EVENTS_WF"));
        placeholders.put("estimates", buildEstimatesClause());

        return templateLoader.loadAndSubstitute("events_wf_receipt", placeholders);
    }

    /**
     * Default implementation returns the REQ/RESP query.
     * For EVENTS_WF, callers should use buildReqRespQuery() and buildReceiptQuery() directly.
     */
    @Override
    public String buildQuery(RunContext ctx, Instant fromInclusive, Instant toExclusive) {
        return buildReqRespQuery(ctx, fromInclusive, toExclusive);
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


