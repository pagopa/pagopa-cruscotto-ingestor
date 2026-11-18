package it.pagopa.cruscotto.ingestion.fdr.persistence;

import java.util.ArrayList;
import java.util.List;

/**
 * Counts the authorised truncations of one file while they happen.
 *
 * <p>Mutable and not thread-safe, like the session that owns it: one instance per file, never
 * shared. It keeps counters and a bounded sample — never the values, which are source data and
 * have no business being accumulated in memory or printed in a log.
 */
final class FdrTruncationTally {

    /**
     * How many row positions are kept.
     *
     * <p>The counts are complete; this is only the handful that travels with them. Without a cap,
     * a day where every causale is over the limit — 40.570 rows is the measured figure for one
     * day — would build one entry per row, in the component whose job is to stay bounded.
     */
    static final int MAX_SAMPLES = 20;

    private long affectedRows;
    private int maxSourceLength;
    private final List<Long> sampleRowOrdinals = new ArrayList<>();

    /**
     * Records one truncation.
     *
     * @param sourceLength length of the source value, in code points
     * @param rowOrdinal   position of the row among those handed to the writer for this file
     */
    void record(int sourceLength, long rowOrdinal) {
        affectedRows++;
        maxSourceLength = Math.max(maxSourceLength, sourceLength);
        if (sampleRowOrdinals.size() < MAX_SAMPLES) {
            sampleRowOrdinals.add(rowOrdinal);
        }
    }

    long affectedRows() {
        return affectedRows;
    }

    FdrTruncationSummary toSummary() {
        return new FdrTruncationSummary("CAUSALE", affectedRows, maxSourceLength,
                FdrCausale.MAX_LENGTH, List.copyOf(sampleRowOrdinals));
    }
}
