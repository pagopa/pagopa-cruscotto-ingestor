package it.pagopa.cruscotto.ingestion.service.ingestion;

import java.util.Map;

public interface EntityTransformer {
    <T> T transform(Map<String, Object> row, Class<T> targetClass) throws TransformationException;

    class TransformationException extends Exception {
        public TransformationException(String message) {
            super(message);
        }

        public TransformationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

