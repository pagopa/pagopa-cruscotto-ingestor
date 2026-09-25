package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Locale;

/**
 * Payment outcome filter values supported by the perimeter query.
 *
 * <p>Maps to the {@code position_tokens.outcome} column: {@code OK} / {@code KO} are stored as-is,
 * while {@link #NO_OUTCOME} matches rows with no recorded outcome (NULL or empty).</p>
 */
public enum PerimeterPaymentStatus {
    OK,
    KO,
    NO_OUTCOME;

    @JsonCreator
    public static PerimeterPaymentStatus fromJson(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT);
        switch (normalized) {
            case "OK":
                return OK;
            case "KO":
                return KO;
            case "NO_OUTCOME":
            case "NESSUN_ESITO":
            case "NO_ESITO":
            case "NONE":
            case "NULL":
                return NO_OUTCOME;
            default:
                throw new IllegalArgumentException("Unsupported payment status: " + value);
        }
    }
}
