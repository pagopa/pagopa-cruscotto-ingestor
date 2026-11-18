package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;

import java.time.LocalDate;
import java.util.List;

/**
 * What writing one file amounted to.
 *
 * <p>Counters only — no rows, and a bounded sample of what went wrong. The orchestration that
 * comes later combines these with the reader's X and N: a row that parsed but was refused by the
 * database is <strong>not</strong> a success, so the day's N is the number written here, not the
 * number the reader mapped. The two must be read together, which is why both results carry the
 * dataset, the day and the blob name.
 *
 * <p>A row whose causale was shortened counts as written: it is in the database, the transformation
 * was authorised, and the loss is recorded in {@code truncation}, not in the rejections.
 *
 * @param rowsReceived      rows handed to the writer
 * @param rowsInserted      rows the database accepted
 * @param rowsRejected      rows the database refused, each one isolated and recorded
 * @param batchesAttempted  JDBC batches started, retries included
 * @param batchesSucceeded  batches that committed
 * @param batchesSplit      batches that failed on something isolable and were halved. Zero on the
 *                          happy path, and the number to watch: a file where it is not zero cost
 *                          more than it should have
 * @param truncation        the authorised shortening of {@code CAUSALE}, aggregated
 * @param errorSample       bounded sample of refused rows; {@code rowsRejected} is the full count
 * @param elapsedMs         wall time of the whole write
 */
public record FdrWriteResult(
        FdrDataset dataset,
        LocalDate businessDay,
        String blobName,
        long rowsReceived,
        long rowsInserted,
        long rowsRejected,
        long batchesAttempted,
        long batchesSucceeded,
        long batchesSplit,
        FdrTruncationSummary truncation,
        List<FdrPersistenceError> errorSample,
        long elapsedMs) {

    /** Whether every row received reached the database. */
    public boolean isComplete() {
        return rowsRejected == 0;
    }
}
