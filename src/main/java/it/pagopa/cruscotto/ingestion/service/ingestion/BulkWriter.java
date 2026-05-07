package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.entity.EntityName;

import java.util.List;

public interface BulkWriter {
    BulkWriteResult writeBulk(EntityName entity, List<?> records) throws BulkWriteException;

    class BulkWriteException extends Exception {
        public BulkWriteException(String message) {
            super(message);
        }

        public BulkWriteException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

