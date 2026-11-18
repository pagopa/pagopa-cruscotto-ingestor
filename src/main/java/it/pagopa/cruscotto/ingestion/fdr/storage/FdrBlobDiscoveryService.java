package it.pagopa.cruscotto.ingestion.fdr.storage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import it.pagopa.cruscotto.ingestion.configuration.AzureBlobProperties;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.util.LogDetails;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Lists the FDR files available for one day on Blob Storage.
 *
 * <p>Metadata only. The content is never opened: no stream, no download, no decompression. That is
 * not an omission to be completed later — the next step opens streams through its own component,
 * and discovery stays cheap enough to run over a day that turns out to be empty.
 *
 * <p><strong>Conditional on the feature flag.</strong> Without
 * {@code @ConditionalOnProperty(fdr.enabled=true)} this bean would be built in every environment,
 * and its constructor would try to build an Azure client from a connection string that non-FDR
 * deployments have no reason to provide — turning a dormant feature into a failed startup. The flag
 * introduced with the FDR configuration exists precisely so that nothing of FDR exists until an
 * environment opts in.
 *
 * <p>The storage account is the shared one, reached through {@code azure.blob.connection-string};
 * only the container is FDR's own. No credential is duplicated, and none is ever logged.
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrBlobDiscoveryService {

    private final BlobContainerClient containerClient;
    private final String container;

    public FdrBlobDiscoveryService(AzureBlobProperties azureBlobProperties, FdrProperties fdrProperties) {
        this(FdrBlobContainerClients.create(azureBlobProperties, fdrProperties),
                fdrProperties.getStorage().getContainer());
    }

    /**
     * Seam for the tests, which supply a stubbed container client.
     *
     * <p>Package-private on purpose: it is the smallest boundary that makes the listing testable
     * without an Azure emulator and without wrapping the SDK in an abstraction of our own, which
     * would be a lot of code to own for no behaviour.
     */
    FdrBlobDiscoveryService(BlobContainerClient containerClient, String container) {
        this.containerClient = containerClient;
        this.container = container;
    }

    /**
     * Everything the day's folder holds, classified.
     *
     * @param day the FDR day; the folder is {@code yyyyMMdd/} and derives from this {@link LocalDate}
     *            alone, so no time zone takes part in the decision
     * @return the files grouped by dataset and the anomalies, never {@code null} and never partial
     * @throws FdrStorageException when the listing itself fails, which is a different outcome from
     *                             an empty day and must stay that way
     */
    public FdrDayDiscovery discover(LocalDate day) {
        String prefix = FdrBlobNameParser.dayPrefix(day);
        long startedAt = System.currentTimeMillis();

        Map<FdrDataset, List<FdrBlobFile>> files = new EnumMap<>(FdrDataset.class);
        for (FdrDataset dataset : FdrDataset.values()) {
            files.put(dataset, new ArrayList<>());
        }
        List<FdrInvalidEntry> invalid = new ArrayList<>();

        for (BlobItem item : listBlobs(prefix, day)) {
            // A hierarchical listing would return folder markers; a flat one should not, but the
            // check costs nothing and a prefix mistaken for a blob would be reported as a bogus
            // anomaly rather than skipped.
            if (Boolean.TRUE.equals(item.isPrefix()) || item.getName() == null
                    || item.getName().endsWith("/")) {
                continue;
            }
            classify(item, day, files, invalid);
        }

        files.values().forEach(list -> list.sort(BY_PROGRESSIVE_THEN_NAME));
        FdrDayDiscovery discovery = new FdrDayDiscovery(day, container, prefix,
                Map.copyOf(files), List.copyOf(invalid));

        logDiscovery(discovery, System.currentTimeMillis() - startedAt);
        return discovery;
    }

    /**
     * Numeric progressive first, blob name as tie-breaker.
     *
     * <p>The numeric comparison is the point: sorted as text, {@code -10} would come before
     * {@code -2} and the files would be processed in an order the customer never numbered them in.
     * The tie-breaker makes the result total, so two runs over the same folder cannot disagree.
     */
    private static final Comparator<FdrBlobFile> BY_PROGRESSIVE_THEN_NAME =
            Comparator.comparing(FdrBlobFile::progressive).thenComparing(FdrBlobFile::blobName);

    private Iterable<BlobItem> listBlobs(String prefix, LocalDate day) {
        try {
            // The SDK's PagedIterable walks every page as it is iterated: no manual continuation
            // token, and no "first page only" truncation, as long as the whole iterable is consumed.
            return containerClient.listBlobs(new ListBlobsOptions().setPrefix(prefix), null);
        } catch (BlobStorageException e) {
            throw new FdrStorageException(kindOf(e),
                    "Unable to list FDR blobs for " + day + " in container " + container, e);
        } catch (RuntimeException e) {
            throw new FdrStorageException(FdrStorageException.Kind.LISTING_FAILED,
                    "Unable to list FDR blobs for " + day + " in container " + container, e);
        }
    }

    private static FdrStorageException.Kind kindOf(BlobStorageException e) {
        return e.getStatusCode() == 404
                ? FdrStorageException.Kind.CONTAINER_NOT_FOUND
                : FdrStorageException.Kind.LISTING_FAILED;
    }

    private static void classify(BlobItem item, LocalDate day,
                                 Map<FdrDataset, List<FdrBlobFile>> files,
                                 List<FdrInvalidEntry> invalid) {
        FdrBlobNameParser.Outcome outcome = FdrBlobNameParser.parse(item.getName(), day);
        if (!outcome.isValid()) {
            invalid.add(outcome.toInvalidEntry());
            return;
        }
        BlobItemProperties properties = item.getProperties();
        files.get(outcome.dataset()).add(new FdrBlobFile(
                item.getName(),
                outcome.dataset(),
                outcome.businessDay(),
                outcome.progressive(),
                readMetadata(properties, BlobItemProperties::getContentLength, -1L),
                readMetadata(properties, BlobItemProperties::getETag, null),
                readMetadata(properties, BlobItemProperties::getLastModified, null)));
    }

    /**
     * Reads one metadata field, falling back when it is unavailable.
     *
     * <p>The null check alone is not enough, and that is not theoretical: {@code getProperties()}
     * can hand back a {@code BlobItemProperties} instance whose internal state was never populated,
     * and the getter then throws instead of returning {@code null}. Guarding only against
     * {@code null} looks careful and defends nothing.
     *
     * <p>Worth the few lines because of what it protects: metadata is decoration here — the name
     * alone decides whether a file is processable — so letting one odd entry abort the listing
     * would fail an entire day over a detail nobody reads. The fallback makes the gap visible
     * ({@code -1} is not a plausible size) rather than papering over it with a zero.
     */
    private static <T> T readMetadata(BlobItemProperties properties,
                                      java.util.function.Function<BlobItemProperties, T> accessor,
                                      T fallback) {
        if (properties == null) {
            return fallback;
        }
        try {
            T value = accessor.apply(properties);
            return value == null ? fallback : value;
        } catch (RuntimeException e) {
            return fallback;
        }
    }

    /**
     * One synthetic line per discovery, as the logging guidance requires for an I/O boundary.
     *
     * <p>Never one line per blob: a day can hold many files and the summary is what gets read. The
     * anomalies are summarised <em>by reason</em>, which is bounded by the size of the enum and
     * therefore safe to put on an INFO line, while the offending names go to DEBUG — enough to know
     * from production that something is off, enough detail on request to know what.
     */
    private static void logDiscovery(FdrDayDiscovery discovery, long elapsedMs) {
        try (MdcScope ignored = MdcScope.phase("FDR_BLOB_DISCOVERY");
             LogDetails details = LogDetails.open()
                     .with(LogFields.DEPENDENCY, "azure-blob")
                     .with("container", discovery.container())
                     // Giornata e prefisso insieme, benche' il secondo contenga il primo: il
                     // prefisso dice dove si e' guardato, la giornata e' il campo su cui si
                     // aggrega. Ricavarla a posteriori significherebbe affettare una stringa.
                     .with("business_day", discovery.businessDay().toString())
                     .with("prefix", discovery.prefix())
                     .with("flussi_files", discovery.countOf(FdrDataset.FLUSSI_RENDICONTAZIONE))
                     .with("iuv_files", discovery.countOf(FdrDataset.IUV_RENDICONTATI))
                     .with("invalid_entries", discovery.invalidEntries().size())
                     .with(LogFields.ELAPSED_MS, elapsedMs)) {
            log.info("Listed FDR blobs for the day");
        }

        if (discovery.invalidEntries().isEmpty()) {
            return;
        }
        logInvalidEntries(discovery);
    }

    /**
     * Una riga di riepilogo delle anomalie, piu' il dettaglio a DEBUG.
     *
     * <p>I conteggi vanno uno per ragione e come <strong>numeri</strong>, non come una mappa resa a
     * stringa: un {@code {UNKNOWN_DATASET=2}} dentro un campo di testo si legge ma non si somma, e
     * la domanda che ci si pone davvero — "quante estensioni sbagliate questa settimana?" — resta
     * senza risposta. Le chiavi sono limitate dalla dimensione dell'enum, quindi la riga non puo'
     * crescere a sorpresa.</p>
     *
     * <p>Un WARN per discovery e non per blob: una giornata puo' contenere molti file, e il
     * riepilogo e' cio' che viene letto. I nomi dei file anomali stanno a DEBUG, cosi' il conteggio
     * dice da produzione che qualcosa non torna e il dettaglio e' disponibile su richiesta.</p>
     */
    private static void logInvalidEntries(FdrDayDiscovery discovery) {
        Map<FdrInvalidEntry.Reason, Long> byReason = new EnumMap<>(FdrInvalidEntry.Reason.class);
        discovery.invalidEntries()
                .forEach(entry -> byReason.merge(entry.reason(), 1L, Long::sum));

        try (MdcScope ignored = MdcScope.phase("FDR_BLOB_DISCOVERY");
             LogDetails details = LogDetails.open()
                     .with("container", discovery.container())
                     .with("business_day", discovery.businessDay().toString())
                     .with("invalid_entries", discovery.invalidEntries().size())) {
            byReason.forEach((reason, count) ->
                    details.with("invalid_" + reason.name().toLowerCase(Locale.ROOT), count));
            log.warn("Found FDR blob entries that cannot be processed");
        }

        discovery.invalidEntries().forEach(entry ->
                log.debug("Skipped unprocessable FDR blob {} ({})", entry.blobName(), entry.reason()));
    }
}
