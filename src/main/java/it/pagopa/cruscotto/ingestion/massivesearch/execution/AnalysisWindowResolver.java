package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    public AnalysisWindowResolver(PerimeterFileRepository perimeterFileRepository, ObjectMapper objectMapper) {
        this.perimeterFileRepository = perimeterFileRepository;
        this.objectMapper = objectMapper;
    }

    /** Resolves the analysis window for the given instance, never {@code null}. */
    public AnalysisWindow resolve(UUID instanceId) {
        Optional<String> filterJson = perimeterFileRepository.readFilterJson(instanceId);
        if (filterJson.isEmpty()) {
            return AnalysisWindow.none();
        }
        PerimeterFilter.PaymentPeriod period;
        try {
            PerimeterFilter filter = objectMapper.readValue(filterJson.orElseThrow(), PerimeterFilter.class);
            period = filter.getPaymentPeriod();
        } catch (Exception e) {
            // JSON malformato: resta permissivo (nessuna finestra), non e' il caso "date sbagliate".
            log.warn("phase=ANALYSIS_WINDOW_PARSE_FAILED instanceId={} reason={}", instanceId, e.getMessage());
            return AnalysisWindow.none();
        }
        if (period == null) {
            return AnalysisWindow.none();
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
        return new AnalysisWindow(from, to);
    }
}
