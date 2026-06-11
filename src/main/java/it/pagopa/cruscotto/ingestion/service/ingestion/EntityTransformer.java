package it.pagopa.cruscotto.ingestion.service.ingestion;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.entity.EntityName;

import java.util.Map;

public interface EntityTransformer {
    <T> T transform(Map<String, Object> row, Class<T> targetClass) throws TransformationException;

    default <T> T transform(Map<String, Object> row, Class<T> targetClass,
                            RunContext ctx, EntityName entity) throws TransformationException {
        return transform(row, targetClass);
    }

    class TransformationException extends Exception {
        public TransformationException(String message) {
            super(message);
        }

        public TransformationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

