package it.pagopa.cruscotto.ingestion.massivesearch.report;

/**
 * Shared SQL fragment applying the optional analysis window to a token alias.
 * Centralized so the three report repositories share a single definition of the predicate.
 *
 * <p>La finestra e' calcolata su {@code inserted_timestamp} (timestamp della sorgente ADX, sempre
 * valorizzato) e non su {@code payment_date}, che e' null per i token non pagati: filtrando su
 * payment_date i tentativi non pagati sparirebbero dalla ricerca per periodo.</p>
 */
public final class ReportWindowSql {

    private ReportWindowSql() {
    }

    /**
     * Optional temporal window predicate on {@code inserted_timestamp} for the given token alias. When
     * the bound parameters ({@code :winFrom} / {@code :winTo}) are {@code null} the predicate is a
     * no-op, so the full history is analysed. Le date sono assolute (nessuna conversione di
     * timezone/offset): {@code inserted_timestamp} e' un {@code TIMESTAMP} senza time zone e i bound
     * sono {@code CAST(... AS timestamp)}, quindi il confronto e' letterale.
     *
     * @param alias the SQL alias of the {@code position_tokens} row to filter
     * @return the {@code AND (...)} fragment to append to a WHERE / JOIN condition
     */
    public static String paymentDateWindow(String alias) {
        return " AND (CAST(:winFrom AS timestamp) IS NULL OR " + alias + ".inserted_timestamp >= CAST(:winFrom AS timestamp))"
            + " AND (CAST(:winTo AS timestamp) IS NULL OR " + alias + ".inserted_timestamp < CAST(:winTo AS timestamp))";
    }
}
