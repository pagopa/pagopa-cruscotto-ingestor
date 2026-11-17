/**
 * FDR Blob Storage access: discovery of the files published for a day.
 *
 * <p>Metadata only. Azure SDK types stop at {@code FdrBlobDiscoveryService}; what leaves this
 * package are plain value objects, which is what keeps the parsing, classification and ordering
 * rules testable without an emulator.
 *
 * <p>The storage account is the shared one configured under {@code azure.blob}; only the container
 * belongs to FDR. Nothing here creates the container, opens a blob or reads content.
 */
package it.pagopa.cruscotto.ingestion.fdr.storage;
