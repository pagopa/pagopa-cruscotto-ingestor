/**
 * Writing the FDR rows into the two partitioned business tables.
 *
 * <p>The last leg of the pipeline: typed rows in, PostgreSQL rows out. Nothing here opens a blob,
 * decompresses or parses — the writer consumes exactly what the reader produced — and nothing here
 * decides the fate of a day: it returns counters, and the orchestration that will come later turns
 * them into a state.
 *
 * <p><strong>Three rules shape everything in this package.</strong>
 *
 * <ol>
 *   <li><strong>Fidelity.</strong> No deduplication, no upsert, no {@code ON CONFLICT}, no default
 *       substituted for a missing value. The real files contain exact duplicates and they must
 *       reach the database, because a duplicate that can be seen is a diagnosable source problem
 *       while one silently dropped is not. The single authorised transformation is {@code CAUSALE},
 *       shortened to 30 characters, and it is applied in one named place.</li>
 *   <li><strong>A bad row must not cost a good batch.</strong> The happy path is one JDBC batch per
 *       chunk; when a batch fails on something isolable, it is rolled back and split in half,
 *       recursively, until the offending rows are alone. Everything else is written. What is
 *       <em>not</em> isolable — a closed connection, a missing partition, a permission error — is
 *       propagated untouched: splitting a batch during a database outage produces thousands of
 *       pointless attempts and hides the real cause.</li>
 *   <li><strong>The day comes from the folder.</strong> {@code DATE_INSERTED} is the business day
 *       carried by {@code FdrBlobFile}, never {@code now()} and never anything derived from the
 *       data; {@code INSERTED_TIMESTAMP} is the source's own value, written with the reading the
 *       file carried and no time-zone arithmetic of any kind — the columns are
 *       {@code TIMESTAMP WITHOUT TIME ZONE} and must hold the same digits as the CSV. A
 *       reingestion run months later must rewrite the same partition with the same timestamps, so
 *       neither value may be touched by the clock.</li>
 * </ol>
 *
 * <p>Memory stays bounded the same way the reader keeps it bounded: rows arrive in batches of
 * {@code fdr.processing.batch-size}, are written and forgotten. What a session keeps for the whole
 * file is counters, plus a bounded sample of truncations and rejections — never the rows.
 */
package it.pagopa.cruscotto.ingestion.fdr.persistence;
