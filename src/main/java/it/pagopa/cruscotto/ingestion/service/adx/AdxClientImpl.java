package it.pagopa.cruscotto.ingestion.service.adx;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultColumn;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.service.ExecutionLogService;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import it.pagopa.cruscotto.ingestion.util.ThrowableDetail;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class AdxClientImpl implements AdxClient {
    private static final String UNIQUE_ID = "UNIQUE_ID";
    private static final String FALLBACK_COLUMN_PREFIX = "col_";
    // Hard cap applied when neither the configured query-timeout nor the duration guardrail
    // yields a value, so the ADX server-side timeout (and the derived client socket read
    // timeout) is ALWAYS bounded and a hung query can never pin a Quartz worker indefinitely.
    private static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofMinutes(8);
    // Minimum guardrail budget worth spending on a query. Below this, the ADX server-timeout would be
    // capped so small that Kusto times out just planning the query (RequestExecutionTimeout) and the
    // run is marked FAILED. Treat such a residual budget as exhausted and stop gracefully instead.
    private static final Duration MIN_USEFUL_QUERY_BUDGET = Duration.ofSeconds(15);

    private final Client kustoClient;
    private final IngestionConfig ingestionConfig;

    @Override
    public AdxQueryResult executeQuery(RunContext ctx, String database, String query) {
        String runId = ctx.getRunId();
        String entityName = ctx.getEntityName();

        Duration remainingDuration = resolveRemainingGuardrailDuration(ctx);
        if (remainingDuration != null && remainingDuration.isZero()) {
            log.warn("ADX_QUERY_SKIPPED runId={} operationId={} entityName={} reason={}",
                    runId, ctx.getOperationId(), entityName, AdxClient.MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR);
            return new AdxQueryResult(false, null, AdxClient.MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR);
        }

        if (database == null || database.isBlank()) {
            return new AdxQueryResult(false, null, "Invalid ADX database: value is blank");
        }
        if (query == null || query.isBlank()) {
            return new AdxQueryResult(false, null, "Invalid ADX query: value is blank");
        }

        long startedAt = System.currentTimeMillis();
        IngestionConfig.AdxConfig.TransientRetryConfig retryConfig = ingestionConfig.getAdx().getTransientRetry();
        int maxAttempts = Math.max(1, retryConfig.getMaxAttempts());
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            // Retries consume time: re-check the (catch-up aware) guardrail budget before each further
            // attempt so a retry can never push the run past its max-duration (which would only get it
            // marked FAILED anyway). The first attempt already passed the pre-check above.
            if (attempt > 1) {
                remainingDuration = resolveRemainingGuardrailDuration(ctx);
                if (remainingDuration != null && remainingDuration.isZero()) {
                    log.warn("ADX_QUERY_SKIPPED runId={} operationId={} entityName={} reason={} beforeRetryAttempt={}",
                            runId, ctx.getOperationId(), entityName,
                            AdxClient.MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR, attempt);
                    return new AdxQueryResult(false, null, AdxClient.MAX_DURATION_GUARDRAIL_EXCEEDED_ERROR);
                }
            }
            // Due orologi, di proposito: nanoTime e' monotono e misura la durata senza risentire
            // degli aggiustamenti NTP del pod; currentTimeMillis da' l'istante assoluto, l'unico
            // correlabile con lo storico di ADX e con gli altri nodi.
            long attemptStartedAtMs = System.currentTimeMillis();
            long attemptStartedNs = System.nanoTime();
            try {
                ClientRequestProperties requestProperties = buildRequestProperties(ctx, remainingDuration);
                KustoOperationResult operationResult = kustoClient.execute(database, query, requestProperties);
                KustoResultSetTable table = operationResult == null ? null : operationResult.getPrimaryResults();
                Map<String, Object> rows = mapRows(table);
                long attemptElapsed = accountAttempt(ctx, attemptStartedNs);
                logAttempt(ctx, database, attempt, maxAttempts, "OK", attemptStartedAtMs, attemptElapsed, rows.size());
                log.info("ADX_CLIENT_SUCCESS runId={} entityName={} database={} rows={} attempt={} totalElapsedMs={}",
                        runId, entityName, database, rows.size(), attempt, System.currentTimeMillis() - startedAt);
                return new AdxQueryResult(true, rows, null);
            } catch (Exception e) {
                long attemptElapsed = accountAttempt(ctx, attemptStartedNs);
                String error = buildErrorMessage(e);
                boolean retryable = attempt < maxAttempts && isTransientError(error);
                logAttempt(ctx, database, attempt, maxAttempts, "KO", attemptStartedAtMs, attemptElapsed, -1);
                log.error("ADX_QUERY_EXECUTION_ERROR runId={} entityName={} database={} attempt={}/{} transient={} error={} totalElapsedMs={}",
                        runId, entityName, database, attempt, maxAttempts, retryable, error,
                        System.currentTimeMillis() - startedAt, e);
                if (!retryable) {
                    return new AdxQueryResult(false, null, error);
                }
                backoffBeforeRetry(attempt, retryConfig, remainingDuration);
            }
        }
        // Unreachable: the loop always returns; kept so the method is exhaustive.
        return new AdxQueryResult(false, null, "ADX query failed after " + maxAttempts + " attempts");
    }

    /**
     * Contabilizza un tentativo ADX nel {@link RunContext}, <strong>qualunque sia l'esito</strong>.
     *
     * @param attemptStartedNs istante di avvio preso da {@link System#nanoTime()} (monotono).
     * @return i millisecondi passati dentro questo tentativo.
     */
    private static long accountAttempt(RunContext ctx, long attemptStartedNs) {
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - attemptStartedNs);
        ctx.addAdxAttemptCount(1);
        ctx.addAdxQueryDurationMs(elapsed);
        return elapsed;
    }

    /**
     * Riga unica per tentativo ADX, pensata per essere aggregata lato log.
     *
     * <p>Porta <strong>inizio ed esito</strong> di ogni singola chiamata, non il totale per
     * esecuzione: e' l'unico modo per ricostruire la <em>concorrenza reale</em>, che richiede gli
     * intervalli e non le somme. Con un marcatore solo per OK e KO l'aggregazione e' una query
     * sola, e i fallimenti — che prima sparivano del tutto — pesano quanto i successi.</p>
     *
     * <p>{@code instanceId} distingue i pod: in produzione sono tre e i loro tentativi si
     * sovrappongono, quindi senza quel campo la concorrenza non e' sommabile.</p>
     *
     * <p>Il campo e' {@code elapsedMs} e misura il <strong>singolo tentativo</strong>, mentre il
     * {@code totalElapsedMs} delle righe ADX_CLIENT_SUCCESS / ADX_QUERY_EXECUTION_ERROR e'
     * cumulativo dal primo tentativo e include i backoff: sommare quello sovrastimerebbe il tempo
     * passato dentro ADX ogni volta che c'e' stato un retry. I nomi sono diversi apposta.</p>
     *
     * <p>Niente testo d'errore: su un fallimento arriverebbe due volte, perche' la riga
     * ADX_QUERY_EXECUTION_ERROR che segue lo riporta per intero (con lo stack) e lo stesso
     * {@code runId} permette di unirle. Qui conta che la riga resti corta e di lunghezza
     * prevedibile, perche' e' telemetria da aggregare. {@code rows=-1} significa "non applicabile",
     * cioe' nessun risultato perche' il tentativo e' fallito.</p>
     */
    private void logAttempt(RunContext ctx, String database, int attempt, int maxAttempts,
                            String outcome, long startedAtMs, long elapsedMs, int rows) {
        log.info("ADX_ATTEMPT runId={} instanceId={} entityName={} database={} outcome={}"
                        + " attempt={}/{} startedAtMs={} endedAtMs={} elapsedMs={} rows={}",
                ctx.getRunId(), ExecutionLogService.getInstanceId(), ctx.getEntityName(),
                database, outcome, attempt, maxAttempts,
                startedAtMs, startedAtMs + elapsedMs, elapsedMs, rows);
    }

    /**
     * {@code true} only for <em>transient</em> failures worth retrying an idempotent read: socket
     * read/connect timeouts, connection resets, throttling, service-unavailable. Result-set-too-large
     * (and other window/limit errors) are deliberately excluded — those are cured by halving the
     * window in {@link AdxQueryService}, not by retrying the same query.
     *
     * <p>Operates on the already-flattened error string ({@link #buildErrorMessage}) to avoid
     * re-rendering the throwable graph on the failure path.</p>
     */
    private boolean isTransientError(String error) {
        if (error == null || error.isEmpty()
                || matchesAny(error, ingestionConfig.getAdx().getWindowTooLargeErrorPatterns())) {
            return false;
        }
        return matchesAny(error, ingestionConfig.getAdx().getTransientRetry().getPatterns());
    }

    private static boolean matchesAny(String text, java.util.List<String> patterns) {
        if (patterns == null) {
            return false;
        }
        String lower = text.toLowerCase(java.util.Locale.ROOT);
        return patterns.stream()
                .filter(pattern -> pattern != null && !pattern.isBlank())
                .anyMatch(pattern -> lower.contains(pattern.toLowerCase(java.util.Locale.ROOT)));
    }

    private void backoffBeforeRetry(int failedAttempt, IngestionConfig.AdxConfig.TransientRetryConfig config,
                                    Duration remaining) {
        Duration base = config.getBaseBackoff() != null ? config.getBaseBackoff() : Duration.ofSeconds(1);
        long baseMillis = Math.max(0, base.toMillis());
        long exponential = baseMillis << Math.min(failedAttempt - 1, 16); // attempt1->base, attempt2->2*base…
        long jitter = baseMillis > 0 ? java.util.concurrent.ThreadLocalRandom.current().nextLong(baseMillis + 1) : 0;
        long sleepMillis = exponential + jitter;
        // Never sleep so long we eat into the budget the retry query itself needs.
        if (remaining != null) {
            long cap = remaining.toMillis() - MIN_USEFUL_QUERY_BUDGET.toMillis();
            sleepMillis = Math.min(sleepMillis, Math.max(0, cap));
        }
        if (sleepMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Identificativi della richiesta verso ADX.
     *
     * <p>Il {@code clientRequestId} porta <strong>entita' e runId</strong> oltre all'UUID. Non e'
     * cosmesi: ADX conserva lo storico delle query con questo campo, quindi il cliente puo'
     * raggruppare il nostro traffico per entita' <em>dentro il suo cluster</em> e — soprattutto —
     * unire riga per riga il suo {@code .show commands-and-queries} al nostro
     * {@code INGEST_EXECUTION_LOG.RUN_ID}. Durante l'incidente del 6-7 ottobre 2026 quel collegamento
     * non c'era e le due fonti si sono dovute incrociare a occhio sugli orari.</p>
     *
     * <p>Formato: {@code cruscotto-ingestor;<ENTITA>;<runId>;<uuid>}. Il prefisso resta invariato
     * perche' e' quello su cui si filtra, e l'UUID resta in coda a garantire l'unicita' della
     * singola richiesta anche fra tentativi della stessa finestra.</p>
     */
    private ClientRequestProperties buildRequestProperties(RunContext ctx, Duration remainingDuration) {
        ClientRequestProperties requestProperties = new ClientRequestProperties();
        requestProperties.setApplication("cruscotto-ingestor");
        requestProperties.setClientRequestId("cruscotto-ingestor;"
                + blankToDash(ctx.getEntityName()) + ";"
                + blankToDash(ctx.getRunId()) + ";"
                + UUID.randomUUID());
        Duration queryTimeout = ingestionConfig.getAdx().getQueryTimeout();
        Duration effectiveTimeout = queryTimeout != null ? queryTimeout : DEFAULT_QUERY_TIMEOUT;
        if (remainingDuration != null && effectiveTimeout.compareTo(remainingDuration) > 0) {
            effectiveTimeout = remainingDuration;
        }
        requestProperties.setTimeoutInMilliSec(Math.max(1, effectiveTimeout.toMillis()));
        return requestProperties;
    }

    /**
     * Il separatore del {@code clientRequestId} e' il punto e virgola: un segmento vuoto o che lo
     * contenga renderebbe la stringa non spezzabile lato ADX, cioe' inutilizzabile proprio per
     * l'aggregazione per cui e' stata messa.
     */
    private static String blankToDash(String value) {
        if (value == null || value.isBlank()) {
            return "-";
        }
        return value.replace(';', '_');
    }

    private Duration resolveRemainingGuardrailDuration(RunContext ctx) {
        IngestionConfig.GuardrailsConfig guardrails = ingestionConfig.getGuardrails();
        if (!guardrails.isEnableMaxDuration() || ctx.getRunStart() == null) {
            return null;
        }

        // Catch-up aware: EVENTS_WF in catch-up has a larger budget (e.g. 60m) than the default (25m).
        // Using the flat default here refused queries at 25m and cut catch-up runs short (marking them
        // FAILED), out of sync with the run loop's guardrail which is already catch-up aware.
        Duration maxDuration = ingestionConfig.resolveMaxDurationForRun(ctx.getEntityName(), ctx.isCatchupMode());
        Duration elapsed = Duration.between(ctx.getRunStart(), Instant.now());
        Duration remaining = maxDuration.minus(elapsed);
        // Zero, negative, or a residual too small to run a real query: treat as exhausted so the caller
        // returns the guardrail sentinel and the run stops gracefully, rather than sending a query whose
        // tiny capped server-timeout would make ADX abort during planning (marking the run FAILED).
        if (remaining.compareTo(MIN_USEFUL_QUERY_BUDGET) <= 0) {
            return Duration.ZERO;
        }
        return remaining;
    }

    private Map<String, Object> mapRows(KustoResultSetTable table) {
        Map<String, Object> rows = new LinkedHashMap<>();
        if (table == null) {
            return rows;
        }

        KustoResultColumn[] tableColumns = table.getColumns();
        List<KustoResultColumn> columns = tableColumns == null ? List.of() : List.of(tableColumns);
        if (log.isDebugEnabled()) {
            log.debug("ADX_RESULT_COLUMNS columns={}", columns.stream().map(this::resolveColumnName).toList());
        }
        Set<String> usedKeys = new HashSet<>();
        int rowIndex = 0;
        while (table.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (KustoResultColumn column : columns) {
                String columnName = resolveColumnName(column);
                row.put(columnName, normalizeValue(table.getObject(column.getOrdinal())));
            }

            String key = ensureUniqueKey(resolveRowKey(row, rowIndex), usedKeys);
            rows.put(key, row);
            if (rowIndex == 0 && log.isDebugEnabled()) {
                log.debug("ADX_RESULT_SAMPLE key={} row={}", key, row);
            }
            rowIndex++;
        }
        return rows;
    }

    private String resolveColumnName(KustoResultColumn column) {
        String name = column.getColumnName();
        if (name == null || name.isBlank()) {
            return FALLBACK_COLUMN_PREFIX + column.getOrdinal();
        }
        return name;
    }

    private String resolveRowKey(Map<String, Object> row, int rowIndex) {
        Object uniqueId = row.get(UNIQUE_ID);
        if (uniqueId != null) {
            String value = String.valueOf(uniqueId).trim();
            if (!value.isEmpty()) {
                return value;
            }
        }
        return "row_" + rowIndex;
    }

    private String ensureUniqueKey(String candidate, Set<String> usedKeys) {
        String key = candidate;
        int suffix = 1;
        while (usedKeys.contains(key)) {
            key = candidate + "#" + suffix;
            suffix++;
        }
        usedKeys.add(key);
        return key;
    }

    private String buildErrorMessage(Exception e) {
        if (containsInvalidClientSecretError(e)) {
            return "Invalid ADX client secret: azure.kusto.app.key must be the secret value, not the secret ID";
        }
        // Kusto wraps the actionable failure in a generic top-level DataServiceException
        // ("...parsing json response...multiple inner exceptions"), while the real signal
        // (e.g. LimitsExceeded / E_QUERY_RESULT_SET_TOO_LARGE for a >64MB result set) lives in the
        // inner exceptions it exposes as an Iterable, not as a cause. Rendering the whole failure graph
        // lets the window-too-large classifier react (halving the window) and makes
        // INGEST_EXECUTION_LOG.ERROR_MESSAGE self-describing.
        return ThrowableDetail.format(e);
    }

    private boolean containsInvalidClientSecretError(Throwable throwable) {
        while (throwable != null) {
            String message = throwable.getMessage();
            if (message != null && message.contains("AADSTS7000215")) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    private Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Instant) {
            return value;
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return zonedDateTime.toInstant();
        }
        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime.toInstant(ZoneOffset.UTC);
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant();
        }
        return value;
    }
}
