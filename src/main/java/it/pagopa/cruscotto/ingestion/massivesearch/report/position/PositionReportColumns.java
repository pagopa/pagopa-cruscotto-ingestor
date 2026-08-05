package it.pagopa.cruscotto.ingestion.massivesearch.report.position;

import java.util.List;

/**
 * Single source of truth for the {@code posizioni.csv} column order and header names.
 *
 * <p>Shared by {@link PositionReportRepository} (which builds the report rows in this order) and
 * {@link PositionCsvExporter} (which writes the header). Column labels mirror the field list defined
 * by the Massive Search requirement.</p>
 */
public final class PositionReportColumns {

    /** Ordered header names, aligned with {@link PositionReportRow#values()}. */
    public static final List<String> HEADERS = List.of(
        "NAV",
        "PA",
        "IUV",
        "CREDITOR_REF_ID",
        "TOKEN_COUNT",
        "OUTCOME",
        "DATE_BORN",
        "DATE_PAYED",
        "IS_PAYED",
        "IS_CART",
        "TOKEN",
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
        "LABEL_PAYMENT_METHOD"
    );

    private PositionReportColumns() {
    }
}
