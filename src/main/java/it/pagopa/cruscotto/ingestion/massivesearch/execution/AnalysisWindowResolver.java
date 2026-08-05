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
        try {
            PerimeterFilter filter = objectMapper.readValue(filterJson.get(), PerimeterFilter.class);
            PerimeterFilter.PaymentPeriod period = filter.getPaymentPeriod();
            if (period == null) {
                return AnalysisWindow.none();
            }
            LocalDateTime from = period.getFrom() == null ? null : period.getFrom().atStartOfDay();
            LocalDateTime to = period.getTo() == null ? null : period.getTo().plusDays(1).atStartOfDay();
            return new AnalysisWindow(from, to);
        } catch (Exception e) {
            log.warn("phase=ANALYSIS_WINDOW_PARSE_FAILED instanceId={} reason={}", instanceId, e.getMessage());
            return AnalysisWindow.none();
        }
    }
}
