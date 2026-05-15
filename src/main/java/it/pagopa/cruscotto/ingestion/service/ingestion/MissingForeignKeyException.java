package it.pagopa.cruscotto.ingestion.service.ingestion;

public class MissingForeignKeyException extends EntityTransformer.TransformationException {

    public MissingForeignKeyException(String message) {
        super(message);
    }
}

