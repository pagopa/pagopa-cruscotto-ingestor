package it.pagopa.cruscotto.ingestion.fdr.persistence;

import java.util.List;

/**
 * What a file's worth of authorised truncations amounts to, in one object.
 *
 * <p>Deliberately <strong>aggregated</strong>. One diagnostic event per shortened causale would
 * mean 40.570 rows in {@code FDR_INGESTION_EVENT} for a single day, which is not observability but
 * noise: the information is the <em>count</em>, because a count that moves is how anyone would
 * notice the source changing shape again. The orchestration can turn this into a single
 * {@code FDR_INGESTION_FILE.ROWS_TRUNCATED} counter: the shortening is the
 * transformation the client asked for, so it is counted and never signalled.
 *
 * @param field            always {@code CAUSALE} today, named explicitly so that the day a second
 *                         field is authorised the event does not become ambiguous
 * @param affectedRows     complete count, not sampled
 * @param maxSourceLength  the longest value seen, in code points. It is the number that says
 *                         whether the column is merely tight or the format changed: 35 is the
 *                         known profile, 70 would be news
 * @param persistedLength  the column width the values were cut to
 * @param sampleRowOrdinals bounded sample of row positions, for troubleshooting only
 */
public record FdrTruncationSummary(
        String field,
        long affectedRows,
        int maxSourceLength,
        int persistedLength,
        List<Long> sampleRowOrdinals) {

    /** Whether anything was truncated at all. */
    public boolean isEmpty() {
        return affectedRows == 0;
    }

    /** The summary of a file where nothing was shortened. */
    public static FdrTruncationSummary none() {
        return new FdrTruncationSummary("CAUSALE", 0, 0, FdrCausale.MAX_LENGTH, List.of());
    }
}
