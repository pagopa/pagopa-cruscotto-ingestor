package it.pagopa.cruscotto.ingestion.massivesearch.report.attempt;

import java.util.List;

/**
 * Single source of truth for the {@code tentativi.csv} column order and header names.
 *
 * <p>Shared by {@link AttemptReportRepository} and {@link AttemptReportGenerator} (which writes the
 * header via the shared {@code CsvLineWriter}). Each row represents a payment attempt (a token).
 * Column labels mirror the field list defined by the Massive Search requirement.</p>
 */
public final class AttemptReportColumns {

    /** Ordered header names, aligned with {@link AttemptReportRow#values()}. */
    public static final List<String> HEADERS = List.of(
        "NAV",
        "PA",
        "IUV",
        "CREDITOR_REF_ID",
        "OUTCOME",
        "TOKEN_COUNT",
        "TOKEN",
        "DATE_BORN",
        "DATE_PAYED",
        "IS_PAYED",
        "IS_CART",
        "TOUCHPOINT",
        "PAYMENT_METHOD",
        "TRANSFER_NUMBER",
        "AMOUNT",
        "PSP",
        "BROKER_PSP",
        "BROKER_PA",
        "STATION",
        "CHANNEL",
        "FEE",
        "ADD_INFO_RRN",
        "ADD_INFO_TID",
        "LABEL_PA",
        "LABEL_PSP",
        "LABEL_BROKER_PA",
        "LABEL_BROKER_PSP",
        "LABEL_TOUCHPOINT",
        "LABEL_PAYMENT_METHOD",
        "HAS_BOLLO"
    );

    private AttemptReportColumns() {
    }
}
