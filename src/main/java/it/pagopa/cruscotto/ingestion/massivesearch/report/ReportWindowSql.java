package it.pagopa.cruscotto.ingestion.massivesearch.report;

/**
 * Shared SQL fragment applying the optional analysis {@code payment_date} window to a token alias.
 * Centralized so the three report repositories share a single definition of the predicate.
 */
public final class ReportWindowSql {

    private ReportWindowSql() {
    }

    /**
     * Optional temporal window predicate on {@code payment_date} for the given token alias. When the
     * bound parameters ({@code :winFrom} / {@code :winTo}) are {@code null} the predicate is a no-op,
     * so the full history is analysed. {@code payment_date} is a {@code TIMESTAMP} (without time zone),
     * hence the {@code CAST(... AS timestamp)}.
     *
     * @param alias the SQL alias of the {@code position_tokens} row to filter
     * @return the {@code AND (...)} fragment to append to a WHERE / JOIN condition
     */
    public static String paymentDateWindow(String alias) {
        return " AND (CAST(:winFrom AS timestamp) IS NULL OR " + alias + ".payment_date >= CAST(:winFrom AS timestamp))"
            + " AND (CAST(:winTo AS timestamp) IS NULL OR " + alias + ".payment_date < CAST(:winTo AS timestamp))";
    }
}
