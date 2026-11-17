package it.pagopa.cruscotto.ingestion.fdr.storage;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * What a day's folder contains: the processable files grouped by dataset, and the entries that are
 * not processable with the reason why.
 *
 * <p><strong>Every dataset is always a key</strong>, mapped to an empty list when it has no files.
 * It is what lets the caller read "FLUSSI 1, IUV 0" as a fact rather than as a missing entry, and
 * it removes the null check that would otherwise sit in front of every use. A day where one dataset
 * never arrived is a real and expected condition, not an error — whether it becomes one is for the
 * orchestration to decide, and nothing here anticipates that.
 *
 * <p><strong>No order between datasets.</strong> Files are grouped, and within a group sorted by
 * numeric progressive. Returning one flat list would quietly make "FLUSSI before IUV" a property of
 * the storage layer, and the workflow would inherit a processing order nobody chose. Choosing it is
 * the workflow's job.
 *
 * <p>An Azure failure is never represented here: it is thrown as {@link FdrStorageException}. That
 * is the whole distinction between "the day is empty" and "we could not look", which otherwise
 * collapse into the same empty result.
 *
 * @param businessDay    the day requested
 * @param container      the container it was looked up in, so a surprising result can be traced to
 *                       a misconfigured environment without reading the configuration
 * @param prefix         the day folder, {@code yyyyMMdd/}
 * @param filesByDataset processable files, one entry per dataset, each sorted by progressive
 * @param invalidEntries everything else found under the prefix, classified
 */
public record FdrDayDiscovery(
        LocalDate businessDay,
        String container,
        String prefix,
        Map<FdrDataset, List<FdrBlobFile>> filesByDataset,
        List<FdrInvalidEntry> invalidEntries) {

    /** Files of one dataset, in processing order. Never {@code null}. */
    public List<FdrBlobFile> filesOf(FdrDataset dataset) {
        return filesByDataset.getOrDefault(dataset, List.of());
    }

    /** How many processable files the dataset has. */
    public int countOf(FdrDataset dataset) {
        return filesOf(dataset).size();
    }

    /** Total processable files across both datasets. */
    public int validCount() {
        return filesByDataset.values().stream().mapToInt(List::size).sum();
    }

    /**
     * {@code true} when the folder yielded nothing processable.
     *
     * <p>Says nothing about why: an empty folder and a folder full of unrecognised files are both
     * empty here, and are told apart by {@link #invalidEntries()}.
     */
    public boolean isEmpty() {
        return validCount() == 0;
    }
}
