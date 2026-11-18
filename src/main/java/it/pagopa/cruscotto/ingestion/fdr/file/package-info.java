/**
 * Reading of the FDR files: GZIP, CSV, and the mapping to the two business rows.
 *
 * <p>The pipeline this package implements is the middle of the FDR one:
 * {@code Blob InputStream -> GZIPInputStream -> CSV streaming -> mapping}. It stops exactly there.
 * Nothing in this package writes to the database, opens a blob or decides the outcome of a day:
 * those belong to the storage package, to the persistence step and to the workflow respectively.
 *
 * <p><strong>Bounded by construction.</strong> A file reaches 400 MB compressed and is never held
 * whole: the stream is decompressed and decoded as it is read, one line at a time, and the rows are
 * handed over in batches of {@code fdr.processing.batch-size}. The only unbounded thing a reader
 * could accumulate is the list of rejected rows, which is why that list is sampled and only the
 * counts are kept in full.
 *
 * <p><strong>What is measured, not assumed.</strong> The CSV dialect here was profiled on 810.492
 * real rows exported from production (the 2026-09-15 day, both datasets):
 * <ul>
 *   <li>separator {@code ,} and RFC-4180 quoting, with {@code ""} as the escaped quote;</li>
 *   <li><em>quoting is per value, not per column</em>: the same column arrives quoted in one file
 *       and bare in another, because the export was run twice with different parameters. A parser
 *       that assumed every field quoted would read the wrong column — it was observed;</li>
 *   <li>the UTF-8 BOM is present in some files and absent in others, in both datasets;</li>
 *   <li>line endings are mixed CRLF and LF, again across files of the same day;</li>
 *   <li>no field spans more than one line;</li>
 *   <li>timestamps carry {@code Z} and a fractional part of 0, 3, 4, 5, 6 or 7 digits.</li>
 * </ul>
 *
 * <p><strong>Timestamps keep the reading the file wrote.</strong> The offset is parsed and then
 * discarded, never applied: the destination columns are {@code TIMESTAMP WITHOUT TIME ZONE}, so
 * what belongs in them is the same digits the source produced. Today every value ends in
 * {@code Z} and the distinction is invisible, which is exactly what makes it worth stating — a
 * conversion would stay unnoticed until the first file with a different offset, and then move a
 * settlement by an hour with nothing in the logs to show for it. A value with no offset at all is
 * read the same way rather than rejected.
 * Everything in this package that looks defensive is one of those observations.
 *
 * <p><strong>Source fidelity.</strong> Values are not trimmed, normalised or shortened here. An
 * empty field becomes {@code null} and that is the only transformation applied to text. The single
 * exception the customer asked for — {@code CAUSALE} shortened to 30 characters — belongs to the
 * persistence step and is deliberately <em>not</em> applied here, so that whatever is written to the
 * database can still be compared with what the file contained.
 */
package it.pagopa.cruscotto.ingestion.fdr.file;
