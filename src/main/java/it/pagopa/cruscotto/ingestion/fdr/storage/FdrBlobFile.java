package it.pagopa.cruscotto.ingestion.fdr.storage;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.OffsetDateTime;

/**
 * A valid FDR file found on Blob Storage, described by its metadata only.
 *
 * <p>There is no content here, and no Azure type: the SDK classes stop at the storage boundary and
 * what crosses into the FDR code is this value object. Keeping it that way means the parsing,
 * ordering and classification logic can be tested without Azure anywhere in sight — which is where
 * the defects actually live.
 *
 * @param blobName  full path inside the container, day folder included
 *                  ({@code 20260915/IUV_RENDICONTATI_20260915-1.csv.gz}). It is what the next step
 *                  needs to open the stream, so it is kept whole rather than split
 * @param dataset   which of the two datasets this file belongs to
 * @param businessDay the FDR day, already verified against the folder it was found in
 * @param progressive the numeric value of the suffix. Ordering uses this and never the string, so
 *                  that 10 follows 2 instead of preceding it. A {@link BigInteger} because the
 *                  customer set no limit on the number of digits: a formally valid file name must
 *                  never become a technical failure of the whole day because the number overflowed.
 *                  Note that it is the <em>value</em>, so {@code -1} and {@code -001} share it while
 *                  remaining two distinct files — the blob name is what tells them apart
 * @param sizeBytes content length as reported by Azure, {@code -1} when unavailable
 * @param eTag      Azure entity tag, useful later to notice a file changed between discovery and read
 * @param lastModified last modification instant as reported by Azure, may be {@code null}
 */
public record FdrBlobFile(
        String blobName,
        FdrDataset dataset,
        LocalDate businessDay,
        BigInteger progressive,
        long sizeBytes,
        String eTag,
        OffsetDateTime lastModified) {
}
