package it.pagopa.cruscotto.ingestion.service.adx;

import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.ingestor.RunPhase;
import it.pagopa.cruscotto.ingestion.ingestor.LogHelper;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.config.AdxTableNamesConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class AdxQueryService {
    private final AdxClient adxClient;
    private final IngestionConfig ingestionConfig;
    private final AdxTableNamesConfig tableNamesConfig;
    private final PositionAdxQueryBuilder positionBuilder;
    private final PositionTokensAdxQueryBuilder positionTokensBuilder;
    private final TransfersAdxQueryBuilder transfersBuilder;
    private final EventsWfAdxQueryBuilder eventsWfBuilder;
    private final ExtraInfoAdxQueryBuilder extraInfoBuilder;

    /**
     * Probes the entity source table for the earliest {@code INSERTED_TIMESTAMP} in the inclusive
     * range {@code [fromInclusive, toInclusive]}, used to skip contiguous empty date ranges by jumping
     * the cursor straight to the next available data.
     *
     * <p>Semantics (kept deliberately unambiguous so the caller never loses data):</p>
     * <ul>
     *   <li>returns a present value → there is data at that timestamp (jump there);</li>
     *   <li>returns empty → the probe succeeded and there is genuinely no data in the range;</li>
     *   <li>throws → the probe failed/was ambiguous; the caller must fall back to the step advance
     *       and must NOT treat it as "no data".</li>
     * </ul>
     *
     * <p>The probe filters only on {@code INSERTED_TIMESTAMP} (a superset of the entity's own row
     * filters), so its minimum can never be later than the first row the real window query would
     * return: jumping to it can skip empty ranges but never skips actual data.</p>
     */
    public Optional<Instant> findNextInsertedTimestamp(RunContext ctx, EntityName entity,
                                                       Instant fromInclusive, Instant toInclusive) {
        String table = tableNamesConfig.getTableName(entity.name());
        // Stessa esclusione degli enti applicata dai template: senza, la probe segnalerebbe "qui ci
        // sono dati" per righe che la query vera poi scarta, il cursore salterebbe li' e troverebbe
        // la finestra vuota — una probe in piu' a ogni ciclo, proprio sull'entita' che il filtro
        // doveva alleggerire. L'invariante della probe regge: il suo risultato resta un
        // sovrainsieme di quello della query vera, che applica anche gli altri filtri.
        String query = "let start=datetime('" + fromInclusive + "');\n"
                + "let end=datetime('" + toInclusive + "');\n"
                + table + "\n"
                + "| where INSERTED_TIMESTAMP between (start .. end)"
                + AdxPaExclusionSql.clause(ingestionConfig.getAdx().getExcludedPaEmittenti()) + "\n"
                + "| summarize NEXT_TS = min(INSERTED_TIMESTAMP)";

        AdxQueryResult result = adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), query);
        if (!result.isSuccess()) {
            if (AdxClient.MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR.equals(result.getError())) {
                // Budget esaurito durante la probe: stop guardrail (non un fallimento della probe).
                throw new AdxGuardrailStopException(ctx.getRunId(), entity.name(), fromInclusive);
            }
            throw new IllegalStateException("empty-window probe failed: " + result.getError());
        }
        if (result.getData() == null || result.getData().isEmpty()) {
            // summarize always yields exactly one row; an empty result is unexpected -> treat as
            // ambiguous and let the caller fall back to the safe step advance.
            throw new IllegalStateException("empty-window probe returned no aggregate row");
        }
        Object rowObject = result.getData().values().iterator().next();
        if (!(rowObject instanceof Map<?, ?> row)) {
            throw new IllegalStateException("empty-window probe returned an unexpected row shape");
        }
        if (!row.containsKey("NEXT_TS")) {
            // Aggregate column missing -> malformed result. Treat as ambiguous (throw) rather than as
            // "no data", so the caller falls back to the safe step advance and never skips a range.
            throw new IllegalStateException("empty-window probe result missing NEXT_TS column");
        }
        Object value = row.get("NEXT_TS");
        if (value == null) {
            return Optional.empty(); // genuinely no data in the range
        }
        // ADX may return the aggregated datetime already typed or as a String; parse tolerantly.
        Optional<Instant> parsed = AdxTimestamps.toInstant(value);
        if (parsed.isPresent()) {
            return parsed;
        }
        // Present but unparseable -> ambiguous: throw so the caller falls back to the step advance
        // (never treat it as "no data", which would skip the range).
        throw new IllegalStateException("empty-window probe NEXT_TS not parseable as timestamp: "
                + value.getClass().getSimpleName() + " value=" + value);
    }

    public Optional<AdxWindowResult> fetchWindow(
            RunContext ctx,
            Instant cursor,
            Duration window,
            Instant endLimit) {

        String runId = ctx.getRunId();
        String entityName = ctx.getEntityName();

        Duration currentWindow = window;
        int attempt = 0;

        while (attempt < ingestionConfig.getMaxWindowHalvingAttempts()) {
            attempt++;

            Instant to = cursor.plus(currentWindow);
            if (to.isAfter(endLimit)) {
                to = endLimit;
            }

            try (LogDetails ignored = windowDetails(cursor, to, currentWindow, attempt)) {
                LogHelper.info(ctx, RunPhase.WINDOW, "Opened source window");
            }

            AdxQueryResult result = executeEntityQuery(ctx, cursor, to);

            if (result.isSuccess()) {
                // WINDOW_SUCCESS e ADX_RESULT sono stati rimossi: erano la TERZA e la quarta riga
                // sullo stesso fatto. L'esito della chiamata lo emette AdxClientImpl per singolo
                // tentativo, e l'esito della finestra con le righe estratte lo emette
                // GenericIngestionRunnerImpl. Qui non restava nulla di nuovo da dire.
                if (log.isDebugEnabled()) {
                    try (LogDetails ignored = LogDetails.open()
                            .with("sample_key", sampleKey(result.getData()))
                            .with("sample_row", sampleRow(result.getData()))) {
                        LogHelper.debug(ctx, RunPhase.WINDOW, "Sampled source window payload");
                    }
                }

                return Optional.of(new AdxWindowResult(
                        cursor,
                        to,
                        currentWindow,
                        attempt,
                        result.getData() != null ? result.getData() : new HashMap<>()
                ));
            }

            // Max-duration budget exhausted mid-run: NOT an error. The run hit its (catch-up aware)
            // time budget, so stop gracefully as a guardrail — the runner maps this to COMPLETED /
            // GUARDRAIL_MAX_DURATION, not FAILED.
            if (AdxClient.MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR.equals(result.getError())) {
                try (LogDetails ignored = windowDetails(cursor, to, currentWindow, attempt)) {
                    LogHelper.info(ctx, RunPhase.SKIP, "Stopped source window on the duration guardrail");
                }
                throw new AdxGuardrailStopException(runId, entityName, cursor);
            }

            // Check if error is result-set-too-large
            if (isResultSetTooLargeError(result.getError())) {
                try (LogDetails ignored = windowDetails(cursor, to, currentWindow, attempt)) {
                    LogHelper.warn(ctx, RunPhase.WINDOW, "Halved the source window, result set too large");
                }

                // Halve the window and retry
                currentWindow = currentWindow.dividedBy(2);
                continue;
            }

            // Other errors: halving would not help, so fail immediately. Raise (instead of returning
            // an empty Optional) so the vendor message reaches INGEST_EXECUTION_LOG via logFailed:
            // no window was read and the checkpoint did not advance, so this must not be recorded as
            // a COMPLETED run.
            try (LogDetails ignored = windowDetails(cursor, to, currentWindow, attempt)
                    .with(LogFields.ERROR_DETAIL, result.getError())) {
                // Nessun throwable da passare: l'eccezione viene costruita qui sotto, e lo stack del
                // guasto vero e' gia' stato emesso da AdxClientImpl sul tentativo che lo ha prodotto.
                LogHelper.error(ctx, RunPhase.ERROR, "Failed source window, error not recoverable by halving");
            }
            throw new AdxQueryFailedException(runId, entityName, cursor, currentWindow, result.getError());
        }

        // Max attempts exceeded
        try (LogDetails ignored = LogDetails.open()
                .with(LogFields.CURSOR, cursor)
                .with(LogFields.WINDOW, currentWindow)
                .with(LogFields.MAX_ATTEMPTS, ingestionConfig.getMaxWindowHalvingAttempts())) {
            LogHelper.error(ctx, RunPhase.ERROR, "Exhausted window halving attempts");
        }

        throw new AdxWindowTooLargeException(runId, entityName, cursor, currentWindow);
    }

    /**
     * {@code true} when the ADX error indicates the window exceeded a service limit (result-set size,
     * record count, memory, throttling), i.e. the cases where halving the window is the right remedy.
     *
     * <p>Matching is on the vendor message, so the recognised substrings are configurable
     * ({@code ingestion.adx.window-too-large-error-patterns}): an unrecognised message falls through
     * to fail-fast, which for a permanent error stalls the entity until the pattern is added. The
     * message itself is always logged as {@code QUERY_ERROR ... error=...}.</p>
     */
    private boolean isResultSetTooLargeError(String error) {
        if (error == null) {
            return false;
        }

        String maxSize = String.valueOf(ingestionConfig.getAdx().getMaxResultSizeMb());
        if (error.contains(maxSize + "MB") || error.contains(maxSize + " MB")) {
            return true;
        }
        List<String> patterns = ingestionConfig.getAdx().getWindowTooLargeErrorPatterns();
        if (patterns == null) {
            return false;
        }
        return patterns.stream()
                .filter(pattern -> pattern != null && !pattern.isBlank())
                .anyMatch(error::contains);
    }

    private String buildQuery(RunContext ctx, Instant cursor, Instant to) {
        EntityName entity = EntityName.valueOf(ctx.getEntityName());

        return switch (entity) {
            case POSITION -> positionBuilder.buildQuery(ctx, cursor, to);
            case POSITION_TOKENS -> positionTokensBuilder.buildQuery(ctx, cursor, to);
            case POSITION_TRANSFERS -> transfersBuilder.buildQuery(ctx, cursor, to);
            case EVENTS_WF -> eventsWfBuilder.buildQuery(ctx, cursor, to);
            case EXTRA_INFO -> extraInfoBuilder.buildQuery(ctx, cursor, to);
            default -> throw new IllegalArgumentException("No query builder configured for entity: " + entity);
        };
    }

    private AdxQueryResult executeEntityQuery(RunContext ctx, Instant cursor, Instant to) {
        EntityName entity = EntityName.valueOf(ctx.getEntityName());
        if (entity == EntityName.EVENTS_WF) {
            return executeEventsWfQueries(ctx, cursor, to);
        }

        String query = buildQuery(ctx, cursor, to);
        logQuerySent(ctx, null, query);
        return adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), query);
    }

    private AdxQueryResult executeEventsWfQueries(RunContext ctx, Instant cursor, Instant to) {
        String reqRespQuery = eventsWfBuilder.buildReqRespQuery(ctx, cursor, to);
        logQuerySent(ctx, "REQ_RESP", reqRespQuery);
        AdxQueryResult reqRespResult = adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), reqRespQuery);
        if (!reqRespResult.isSuccess()) {
            return new AdxQueryResult(false, null, reqRespResult.getError());
        }

        String receiptQuery = eventsWfBuilder.buildReceiptQuery(ctx, cursor, to);
        logQuerySent(ctx, "RECEIPT", receiptQuery);
        AdxQueryResult receiptResult = adxClient.executeQuery(ctx, ingestionConfig.getAdx().getDatabase(), receiptQuery);
        if (!receiptResult.isSuccess()) {
            return new AdxQueryResult(false, null, receiptResult.getError());
        }

        Map<String, Object> mergedRows = mergeRows(reqRespResult.getData(), receiptResult.getData());
        // Questa riga NON e' un duplicato dell'esito di AdxClientImpl: riporta la fusione di due
        // query distinte in un solo insieme di righe, che nessun altro punto conosce.
        try (LogDetails ignored = LogDetails.open()
                .with("req_resp_rows", countExtractedRows(reqRespResult.getData()))
                .with("receipt_rows", countExtractedRows(receiptResult.getData()))
                .with("merged_rows", mergedRows.size())) {
            LogHelper.info(ctx, RunPhase.WINDOW, "Merged request/response and receipt rows");
        }
        return new AdxQueryResult(true, mergedRows, null);
    }

    /**
     * I dettagli che descrivono una finestra, condivisi da tutte le righe che la riguardano.
     *
     * <p>Un factory e non quattro {@code with()} ripetuti a ogni punto di log: le stesse quattro
     * chiavi, scritte a mano cinque volte, sono il modo tipico in cui due righe della stessa
     * famiglia finiscono con nomi diversi e smettono di essere confrontabili.
     */
    private static LogDetails windowDetails(Instant cursor, Instant to, Duration window, int attempt) {
        return LogDetails.open()
                .with(LogFields.CURSOR, cursor)
                .with(LogFields.TO, to)
                .with(LogFields.WINDOW, window)
                .with(LogFields.ATTEMPT, attempt);
    }

    /**
     * La query inviata, a {@code DEBUG}.
     *
     * <p>E' un log di <strong>intenzione</strong>, quindi fuori da {@code INFO} per la regola del
     * fatto compiuto: l'esito della chiamata e' la riga che emette {@code AdxClientImpl}. Resta
     * perche' lunghezza e hash della query sono l'unico modo di capire, da un log di produzione,
     * se due esecuzioni hanno inviato la stessa query o una diversa.
     */
    private void logQuerySent(RunContext ctx, String type, String query) {
        if (!log.isDebugEnabled()) {
            return;
        }
        try (LogDetails ignored = LogDetails.open()
                .with(LogFields.DEPENDENCY, "ADX")
                .with("query_type", type)
                .with("query_length", query.length())
                .with("query_hash", Integer.toHexString(query.hashCode()))) {
            LogHelper.debug(ctx, RunPhase.WINDOW, "Sent query to ADX");
        }
    }

    private Map<String, Object> mergeRows(Map<String, Object> first, Map<String, Object> second) {
        Map<String, Object> merged = new LinkedHashMap<>();
        appendRows(merged, first);
        appendRows(merged, second);
        return merged;
    }

    private void appendRows(Map<String, Object> target, Map<String, Object> source) {
        if (source == null || source.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            String uniqueKey = key;
            int suffix = 1;
            while (target.containsKey(uniqueKey)) {
                uniqueKey = key + "#" + suffix;
                suffix++;
            }
            target.put(uniqueKey, entry.getValue());
        }
    }

    private int countExtractedRows(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return 0;
        }
        return data.size();
    }

    private String sampleKey(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return "n/a";
        }
        return data.keySet().iterator().next();
    }

    private Object sampleRow(Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return "n/a";
        }
        return data.values().iterator().next();
    }
}

