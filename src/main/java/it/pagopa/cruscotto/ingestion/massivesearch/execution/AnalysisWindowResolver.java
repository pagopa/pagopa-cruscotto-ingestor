package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import it.pagopa.cruscotto.ingestion.util.MdcScope;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterFileRepository;
import it.pagopa.cruscotto.ingestion.massivesearch.perimeter.PerimeterFilter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * Resolves the optional {@link AnalysisWindow} of a search instance from
 * {@code search_filter.filter_json} ({@code paymentPeriod}).
 *
 * <p>The period is optional and shared by both instance types: for FILTER instances it is already
 * used to build the perimeter, for CSV instances it corresponds to the optional analysis period the
 * operator may set. When no filter row or no period is present, an empty window is returned and the
 * full history is analysed.</p>
 */
@Slf4j
@Component
public class AnalysisWindowResolver {

    private final PerimeterFileRepository perimeterFileRepository;
    private final ObjectMapper objectMapper;
    private final MassiveSearchProperties properties;

    public AnalysisWindowResolver(PerimeterFileRepository perimeterFileRepository, ObjectMapper objectMapper,
                                  MassiveSearchProperties properties) {
        this.perimeterFileRepository = perimeterFileRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    /** Resolves the analysis window for the given instance, never {@code null}. */
    public AnalysisWindow resolve(UUID instanceId) {
        Resolved resolved = doResolve(instanceId);
        logWindow(instanceId, resolved);
        return resolved.window();
    }

    /**
     * Log esplicito della finestra risolta: e' il filtro che, se mal configurato, azzera i report in
     * modo silenzioso, quindi deve essere sempre ricostruibile dai log.
     */
    private void logWindow(UUID instanceId, Resolved resolved) {
        AnalysisWindow window = resolved.window();
        try (MdcScope ignored = MdcScope.phase("ANALYSIS_WINDOW")
                     .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH")
                     .with(LogFields.CTX_INSTANCE_ID, String.valueOf(instanceId));
             LogDetails details = LogDetails.open()
                     .with(LogFields.FROM, window.fromInclusive())
                     .with(LogFields.TO, window.toExclusive())
                     .with("source", resolved.source())
                     .with("default_lookback_months", properties.getExecution().getDefaultLookbackMonths())) {
            log.info("Resolved the analysis window");
        }
        if (resolved.source() == WindowSource.DEFAULT_LOOKBACK) {
            try (MdcScope ignored = MdcScope.phase("ANALYSIS_WINDOW")
                         .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH")
                         .with(LogFields.CTX_INSTANCE_ID, String.valueOf(instanceId));
                 LogDetails details = LogDetails.open()
                         .with(LogFields.FROM, window.fromInclusive())) {
                log.warn("The instance carries no period: only rows from the default lookback are "
                        + "analysed. Where data ageing is not active the reports can come out empty");
            }
        }
    }

    private Resolved doResolve(UUID instanceId) {
        Optional<String> filterJson = perimeterFileRepository.readFilterJson(instanceId);
        if (filterJson.isEmpty()) {
            return defaultWindow(null);
        }
        PerimeterFilter.PaymentPeriod period;
        try {
            PerimeterFilter filter = objectMapper.readValue(filterJson.orElseThrow(), PerimeterFilter.class);
            period = filter.getPaymentPeriod();
        } catch (Exception e) {
            // JSON malformato: resta permissivo (nessuna finestra), non e' il caso "date sbagliate".
            try (MdcScope ignored = MdcScope.phase("ANALYSIS_WINDOW_PARSE_FAILED")
                         .with(LogFields.EVENT_ACTION, "MASSIVE_SEARCH")
                         .with(LogFields.CTX_INSTANCE_ID, String.valueOf(instanceId));
                 LogDetails details = LogDetails.open().with(LogFields.ERROR_DETAIL, e.getMessage())) {
                log.warn("Could not parse the instance period, falling back to the default window");
            }
            return defaultWindow(null);
        }
        if (period == null) {
            return defaultWindow(null);
        }
        // datetime al secondo (allineato al BE): from inclusivo, to esclusivo, senza troncamento al giorno
        LocalDateTime from = period.getFrom();
        LocalDateTime to = period.getTo();
        // Validazione paymentPeriod: i bound singoli null sono ammessi (range aperto), ma se entrambi
        // presenti 'from' deve precedere 'to'. from >= to (date invertite o range degenere) e' un input
        // sbagliato: fallisce lo step ANALYSIS_WINDOW -> esecuzione FAILED, invece di produrre un report
        // vuoto silenzioso.
        if (from != null && to != null && !from.isBefore(to)) {
            throw new MassiveSearchExecutionException(
                "Invalid paymentPeriod for instance " + instanceId
                    + ": 'from' (" + from + ") must be strictly before 'to' (" + to + ")");
        }
        if (from == null) {
            return defaultWindow(to);
        }
        return new Resolved(new AnalysisWindow(from, to), WindowSource.PAYMENT_PERIOD);
    }

    /**
     * Finestra applicata quando l'istanza non fissa il bound inferiore (istanze CSV, filtri senza
     * paymentPeriod): la ricerca considera solo gli ultimi N mesi.
     *
     * <p>E' una scelta funzionale, non un'ottimizzazione: seleziona la finestra di dati online.
     * Il default viene omesso se non precede il bound superiore richiesto dall'utente, per non
     * trasformare un periodo legittimo piu' vecchio del lookback in un range vuoto.</p>
     */
    private Resolved defaultWindow(LocalDateTime toExclusive) {
        int months = properties.getExecution().getDefaultLookbackMonths();
        WindowSource source = toExclusive == null ? WindowSource.NONE : WindowSource.PAYMENT_PERIOD;
        if (months <= 0) {
            return new Resolved(
                toExclusive == null ? AnalysisWindow.none() : new AnalysisWindow(null, toExclusive), source);
        }
        LocalDateTime from = LocalDateTime.now().minusMonths(months);
        if (toExclusive != null && !from.isBefore(toExclusive)) {
            return new Resolved(new AnalysisWindow(null, toExclusive), source);
        }
        return new Resolved(new AnalysisWindow(from, toExclusive), WindowSource.DEFAULT_LOOKBACK);
    }

    /** Origine del bound inferiore, usata solo per la diagnostica nei log. */
    private enum WindowSource {
        /** Periodo indicato esplicitamente dall'istanza. */
        PAYMENT_PERIOD,
        /** Bound inferiore imposto dal lookback di default. */
        DEFAULT_LOOKBACK,
        /** Nessun bound: viene analizzata tutta la storia. */
        NONE
    }

    private record Resolved(AnalysisWindow window, WindowSource source) {
    }
}
