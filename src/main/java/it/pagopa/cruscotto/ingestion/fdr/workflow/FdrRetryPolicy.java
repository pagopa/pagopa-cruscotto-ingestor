package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrFileFormatException;
import it.pagopa.cruscotto.ingestion.fdr.persistence.FdrPersistenceException;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrStorageException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Decide se un guasto merita un altro tentativo, e quando.
 *
 * <p>La distinzione è la stessa che governa il fallback del writer, applicata un livello più in
 * alto: **ritentare serve solo se la prossima lettura può andare diversamente**. Un container
 * irraggiungibile, un database giù, un download interrotto: sì. Un dataset che non esiste, un
 * header che non corrisponde, una partizione mancante: no, e insistere cinque volte a un'ora di
 * distanza sposta solo di cinque ore il momento in cui qualcuno guarda il problema.
 *
 * <p>Il massimo dei tentativi viene da {@code fdr.retry.max-attempts} e l'intervallo da
 * {@code fdr.retry.interval}: nessuno dei due è scritto nel codice, perché sono una politica del
 * cliente e non una proprietà del meccanismo.
 */
@Component
@ConditionalOnProperty(prefix = "fdr", name = "enabled", havingValue = "true")
public class FdrRetryPolicy {

    private final int maxAttempts;
    private final java.time.Duration interval;

    public FdrRetryPolicy(FdrProperties fdrProperties) {
        this.maxAttempts = fdrProperties.getRetry().getMaxAttempts();
        this.interval = fdrProperties.getRetry().getInterval();
    }

    /**
     * Se il guasto è transitorio.
     *
     * <p>L'ordine dei controlli è una classifica di precisione: prima i tipi FDR, che il guasto lo
     * hanno già classificato alla fonte, poi il resto. Ciò che non si riconosce <em>non</em> è
     * ritentabile — all'opposto della regola del writer, e per lo stesso motivo: lì il rischio era
     * una tempesta di tentativi, qui è una giornata che resta in attesa per cinque ore per poi
     * fallire comunque, nascondendo un difetto che andava guardato subito.
     */
    public boolean isRetryable(Throwable failure) {
        if (failure instanceof FdrStorageException storage) {
            // Il container che non esiste è configurazione: nessun numero di tentativi lo crea.
            return storage.kind() != FdrStorageException.Kind.CONTAINER_NOT_FOUND;
        }
        if (failure instanceof FdrFileFormatException format) {
            return format.kind() == FdrFileFormatException.Kind.CORRUPT_ARCHIVE
                    || format.kind() == FdrFileFormatException.Kind.READ_FAILED;
        }
        if (failure instanceof FdrPersistenceException) {
            // Partizione mancante e dataset sbagliato sono configurazione o difetto nostro.
            return false;
        }
        return failure instanceof DataAccessException;
    }

    /** Se, consumato questo tentativo, ne resta almeno un altro. */
    public boolean hasAttemptsLeft(int attemptCount) {
        return attemptCount < maxAttempts;
    }

    public Instant nextAttemptAt(Instant now) {
        return now.plus(interval);
    }

    public int maxAttempts() {
        return maxAttempts;
    }
}
