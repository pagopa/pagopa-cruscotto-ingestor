package it.pagopa.cruscotto.ingestion.massivesearch.report.transfer;

import java.util.List;

/**
 * Single source of truth for the {@code versamenti.csv} column order and header names.
 *
 * <p>Shared by {@link TransferReportRepository} and {@link TransferCsvExporter}. Each row represents a
 * single transfer (versamento) of a payment attempt. Column labels mirror the field list defined by
 * the Massive Search requirement.</p>
 */
public final class TransferReportColumns {

    /** Ordered header names, aligned with {@link TransferReportRow#values()}. */
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
        "TRANSFER_ID",
        "TRANSFER_AMOUNT",
        "TRANSFER_IBAN",
        "TRANSFER_TYPE",
        "TRANSFER_PA",
        "LABEL_PA",
        "LABEL_PSP",
        "LABEL_BROKER_PA",
        "LABEL_BROKER_PSP",
        "LABEL_TOUCHPOINT",
        "LABEL_PAYMENT_METHOD",
        "HAS_BOLLO"
    );

    private TransferReportColumns() {
    }
}
