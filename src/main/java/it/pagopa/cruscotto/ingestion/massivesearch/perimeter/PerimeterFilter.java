package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/**
 * Deserialized view of {@code search_filter.filter_json} for a FILTER search instance.
 *
 * <p>All fields are optional: a missing/empty field means "no restriction on that dimension".
 * Unknown properties are ignored so the contract can evolve without breaking existing instances.</p>
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class PerimeterFilter {

    /** Payment period (maps to {@code position_tokens.payment_date}). */
    private PaymentPeriod paymentPeriod;

    /** Payment outcome selection (OK / KO / no outcome). */
    private List<PerimeterPaymentStatus> paymentStatuses;

    /** Touchpoints ({@code position_tokens.touchpoint}). */
    private List<String> touchpoints;

    /** Payment methods ({@code position_tokens.payment_method}). */
    private List<String> paymentMethods;

    /** Amount, punctual or range ({@code position_tokens.amount}). */
    private AmountFilter amount;

    /** Creditor institutions / ente creditore ({@code position.pa_emittente}). */
    private List<String> creditors;

    /** PSP ids ({@code position_tokens.psp}). */
    private List<Integer> psps;

    /** Technological partners / intermediaries ({@code position_tokens.intermediario_pa|intermediario_psp}). */
    private List<Integer> technologicalPartners;

    /** Channels ({@code position_tokens.canale}). */
    private List<Integer> channels;

    /** Stations ({@code position_tokens.stazione}). */
    private List<Integer> stations;

    /** Inclusive payment period boundaries. */
    @Getter
    @Setter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PaymentPeriod {
        private LocalDate from;
        private LocalDate to;
    }

    /** Punctual amount ({@code exact}) or interval ({@code min}/{@code max}). */
    @Getter
    @Setter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AmountFilter {
        private BigDecimal exact;
        private BigDecimal min;
        private BigDecimal max;
    }
}
