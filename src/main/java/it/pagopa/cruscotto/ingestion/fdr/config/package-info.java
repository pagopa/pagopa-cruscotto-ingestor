/**
 * FDR configuration and {@code fdr:} properties binding.
 *
 * <p>Holds the feature toggle, the dedicated Blob container and the processing/retry knobs. The
 * storage account credentials are deliberately absent: they live once under {@code azure.blob} and
 * are shared, so the secret is never duplicated here.
 */
package it.pagopa.cruscotto.ingestion.fdr.config;
