package it.pagopa.cruscotto.ingestion.util;

/**
 * Scrive i dettagli di una milestone, che finiranno nell'oggetto {@code ctx_details}.
 *
 * <p>Si usa come uno scope, perche' i dettagli valgono per <strong>una</strong> riga di log e non
 * devono sopravviverle:
 *
 * <pre>{@code
 * try (LogDetails d = LogDetails.open()
 *         .with(LogFields.CURSOR, cursor)
 *         .with(LogFields.ROWS, rowsProcessed)) {
 *     LogHelper.info(ctx, RunPhase.WINDOW, "Advanced ingestion window");
 * }
 * }</pre>
 *
 * <p><strong>Il tipo lo decide il tipo Java.</strong> Un {@code Number} finisce sotto il prefisso
 * numerico, un {@code Boolean} sotto quello booleano, tutto il resto sotto quello testuale. Da qui
 * il customizer lo riporta al tipo giusto nel JSON. Non c'e' nessuna tabella da aggiornare e
 * nessun modo di sbagliare il tipo di un campo nuovo: e' il punto per cui questo meccanismo esiste
 * al posto di un registro chiave/tipo.
 *
 * <p>Un valore {@code null} <strong>non</strong> scrive la chiave. Un campo assente e' un campo
 * assente; uno zero o una stringa "null" sarebbero una misura falsa.
 *
 * @see LogFields il dizionario dei nomi
 */
public final class LogDetails implements AutoCloseable {

    private final MdcScope scope = MdcScope.open();

    private LogDetails() {
    }

    public static LogDetails open() {
        return new LogDetails();
    }

    /**
     * Aggiunge un dettaglio alla riga di log che verra' emessa dentro questo scope.
     *
     * @param name  nome della chiave dentro {@code ctx_details}; usare le costanti di
     *              {@link LogFields} quando esistono, per non avere lo stesso dato sotto due nomi
     * @param value {@code null} per non scrivere la chiave
     */
    public LogDetails with(String name, Object value) {
        if (name == null || value == null) {
            return this;
        }
        scope.with(prefixFor(value) + name, String.valueOf(value));
        return this;
    }

    private static String prefixFor(Object value) {
        if (value instanceof Number) {
            return LogFields.DETAIL_NUMERIC_PREFIX;
        }
        if (value instanceof Boolean) {
            return LogFields.DETAIL_BOOLEAN_PREFIX;
        }
        return LogFields.DETAIL_TEXT_PREFIX;
    }

    @Override
    public void close() {
        scope.close();
    }
}
