package it.pagopa.cruscotto.ingestion.fdr.file;

import java.util.List;

/**
 * Receives the mapped rows, a batch at a time.
 *
 * <p>The reader never collects a file: it fills a batch of {@code fdr.processing.batch-size} rows,
 * hands it over, and forgets it. The handler is therefore what decides the memory profile of a
 * 400 MB file — keeping a reference to the batches would put the whole file back in the heap, one
 * list at a time.
 *
 * <p>Each batch is a fresh list, which the handler may keep or modify. Reusing one list across
 * batches would save an allocation per thousand rows and introduce the aliasing defect where a
 * handler that stores the batch finds it emptied underneath.
 *
 * <p><strong>Throwing stops the file.</strong> A failure here is not a bad row, it is the
 * destination refusing the data, and carrying on would read a whole file to write none of it.
 */
@FunctionalInterface
public interface FdrBatchHandler<T extends FdrBusinessRow> {

    /** @param batch never empty, never {@code null} */
    void accept(List<T> batch);
}
