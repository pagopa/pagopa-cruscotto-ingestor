package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * What reading one file produced.
 *
 * <p>This is the X and N of the outcome rules, measured per file: {@code dataRows} is X, the number
 * of data lines the file contained, and {@code mappedRows} is N, the number that became business
 * rows. The workflow sums them across the files of a dataset and decides the day's state on the
 * totals — a single file never decides it, which is why nothing here is called {@code COMPLETED} or
 * {@code FAILED}.
 *
 * @param dataRows            X: data lines, blank lines excluded, header excluded
 * @param mappedRows          N: rows handed to the batch handler
 * @param rejectedRows        {@code dataRows - mappedRows}, kept explicit so the invariant can be
 *                            asserted rather than assumed
 * @param blankLines          skipped empty lines. Counted and not ignored: a file that is suddenly
 *                            half blank lines is a different file, and the counter is the only
 *                            thing that would show it
 * @param rejectionsByReason  complete counts — every rejection is counted, including those not
 *                            sampled
 * @param rejectionSample     the first few rejections, bounded. A file can reject six hundred
 *                            thousand rows and the diagnosis needs a handful of them, not a heap
 *                            full
 * @param elapsedMs           wall time of the whole read, decompression included
 */
public record FdrFileReadResult(
        FdrDataset dataset,
        String blobName,
        LocalDate businessDay,
        long dataRows,
        long mappedRows,
        long rejectedRows,
        long blankLines,
        Map<FdrRowRejection.Reason, Long> rejectionsByReason,
        List<FdrRowRejection> rejectionSample,
        long elapsedMs) {

    /**
     * A valid file that carried no data row.
     *
     * <p>Not a failure on its own. The exports are produced on a schedule, and a window with no
     * traffic yields a file with a header and nothing under it; it becomes a problem only if
     * <em>every</em> file of the dataset is like this, which is a judgement the day's aggregation
     * makes, not this one.
     */
    public boolean isEmptySource() {
        return dataRows == 0;
    }

    /** Whether every data row became a business row: X == N. */
    public boolean isComplete() {
        return rejectedRows == 0;
    }
}
