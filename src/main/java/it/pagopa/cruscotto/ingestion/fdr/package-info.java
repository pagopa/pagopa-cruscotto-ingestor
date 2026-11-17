/**
 * FDR bounded context (Flussi di Rendicontazione).
 *
 * <p>Self-contained bounded context living inside the {@code cruscotto-ingestor} service but kept
 * separate from both the ADX ingestion pipeline and Massive Search. Classes here must not modify or
 * reuse the existing ingestion runners, ADX transformers, checkpoint logic or Quartz ingestion jobs.
 *
 * <p>The feature ingests the daily reporting flows published on Azure Blob Storage as GZIP-compressed
 * CSV ({@code .csv.gz}, never ZIP) into two partitioned business tables,
 * {@code FLUSSI_RENDICONTAZIONE} (the flow revisions) and {@code IUV_RENDICONTATI} (the transfers),
 * plus three operational tables that track the request, the per-day state and the audit trail.
 *
 * <p>Two invariants decide most of the design and are easy to break by accident:
 * <ul>
 *   <li><strong>Source fidelity.</strong> An anomalous FDR must be ingested, flagged and diagnosed —
 *       never rejected. Any constraint able to fail an insert is a constraint able to lose data, which
 *       is why the business tables carry no foreign key, no unique constraint and no {@code NOT NULL}
 *       on source columns, and why truncation is forbidden there.</li>
 *   <li><strong>A reingestion rebuilds the data, it does not re-date it.</strong>
 *       {@code DATE_INSERTED} is the FDR business day (and the partition key) and
 *       {@code INSERTED_TIMESTAMP} is the timestamp carried by the source; reprocessing a past day
 *       must reproduce both. Stamping them with the current instant would make the day move between
 *       partitions instead of being replaced.</li>
 * </ul>
 *
 * <p>Timestamps that describe <em>our</em> processing belong to the operational tables, never to the
 * business columns.
 */
package it.pagopa.cruscotto.ingestion.fdr;
