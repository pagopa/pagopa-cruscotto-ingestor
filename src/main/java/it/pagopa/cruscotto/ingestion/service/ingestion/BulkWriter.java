package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.entity.EntityName;

import java.util.List;

public interface BulkWriter {

    /**
     * Esegue bulk insert atomica via JdbcTemplate.batchUpdate.
     * Successo → BULK_OK log + BulkWriteResult.
     * Errore SQL → BULK_KO_TOTAL log + BulkWriteException (NON aggiornare checkpoint, NON staging).
     */
    BulkWriteResult writeBulk(EntityName entity, List<?> records, String runId) throws BulkWriteException;

    class BulkWriteException extends Exception {
        public BulkWriteException(String message) {
            super(message);
        }

        public BulkWriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

