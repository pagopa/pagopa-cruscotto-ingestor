package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.LocalDateTime;
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

    /** Payment period; applicato su {@code position_tokens.inserted_timestamp} (sorgente ADX, sempre
     * valorizzato) e non su payment_date, che e' null per i token non pagati. */
    private PaymentPeriod paymentPeriod;

    /** Payment outcome selection (OK / KO / no outcome). */
    private List<PerimeterPaymentStatus> paymentStatuses;

    /** Touchpoints ({@code position_tokens.touchpoint}). */
    private List<String> touchpoints;

    /** Payment methods ({@code position_tokens.payment_method}). */
    private List<String> paymentMethods;

    /** Amount, punctual or range ({@code position_tokens.amount}). */
    private AmountFilter amount;

    /**
     * Creditor institutions / ente creditore: id di {@code anag_pa_emittente} (allineato al BE, che
     * invia gli id di anagrafica come per psps/channels/stations). Il perimetro risolve gli id nei
     * rispettivi {@code codice} per confrontarli con {@code position.pa_emittente}, che contiene il
     * codice testuale e non l'id.
     */
    private List<Integer> creditors;

    /** PSP ids ({@code position_tokens.psp}). */
    private List<Integer> psps;

    /** Technological partners / intermediaries ({@code position_tokens.intermediario_pa|intermediario_psp}). */
    private List<Integer> technologicalPartners;

    /** Channels ({@code position_tokens.canale}). */
    private List<Integer> channels;

    /** Stations ({@code position_tokens.stazione}). */
    private List<Integer> stations;

    /**
     * Payment period boundaries, con precisione al secondo (allineato al BE: filtro anche per ore/min/sec).
     * Convenzione: {@code from} inclusivo, {@code to} esclusivo (coerente con AnalysisWindow/ReportWindowSql).
     */
    @Getter
    @Setter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PaymentPeriod {
        private LocalDateTime from;
        private LocalDateTime to;
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
