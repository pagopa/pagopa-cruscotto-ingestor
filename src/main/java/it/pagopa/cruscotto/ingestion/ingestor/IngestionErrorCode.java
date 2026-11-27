package it.pagopa.cruscotto.ingestion.ingestor;

/**
 * Il vocabolario di {@code STG_INGEST_ERROR.ERROR_CODE}.
 *
 * <p>Si scrive soltanto: il valore finisce in colonna come stringa e non viene mai riletto con
 * {@code valueOf}. Toglierne uno non rompe quindi la lettura delle righe storiche, ma le lascia
 * senza una voce che le spieghi.
 */
public enum IngestionErrorCode {
    MISSING_FOREIGN_KEY,
    TRANSFORMATION_ERROR,
    BULK_WRITE_ERROR,
    VALIDATION_ERROR,

    /**
     * Non viene piu' scritto da nessuno, ed e' voluto.
     *
     * <p>Lo produceva {@code insertDiscardedBulk} per le EXTRA_INFO fuori whitelist, rimosso il
     * 2026-11-26 su decisione del cliente: una riga scartata di proposito va scartata, non
     * archiviata. Su dev erano oltre due milioni di righe al giorno con il payload intero, fino a
     * esaurire lo storage.
     *
     * <p><strong>Resta dichiarato di proposito</strong>, e non e' codice morto dimenticato: il
     * valore esiste ancora nelle righe gia' scritte — in produzione per tutta la durata della
     * retention — e questo enum e' l'unico posto in cui quei codici sono elencati. Chi raggruppa
     * per {@code ERROR_CODE} lo incontra, e deve poter capire cosa significava.
     */
    BUSINESS_RULE_DISCARDED,

    UNEXPECTED_ERROR
}
