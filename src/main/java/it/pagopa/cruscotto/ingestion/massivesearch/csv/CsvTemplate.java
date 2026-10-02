package it.pagopa.cruscotto.ingestion.massivesearch.csv;

/**
 * Supported Massive Search CSV templates. Mirrors the {@code search_perimeter_file.template} domain.
 */
public enum CsvTemplate {
    /** NAV + idDominio/PA. */
    NAV_PA,
    /** IUV + idDominio/PA. */
    IUV_PA,
    /** Only NAV. */
    NAV,
    /** Only IUV. */
    IUV,
    /** Only TOKEN. */
    TOKEN,
    /** Header not recognized. */
    UNKNOWN
}
