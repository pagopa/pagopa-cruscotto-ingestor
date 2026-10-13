package it.pagopa.cruscotto.ingestion.ingestor;

import it.pagopa.cruscotto.ingestion.batch.RunContext;
import it.pagopa.cruscotto.ingestion.util.LogFields;
import it.pagopa.cruscotto.ingestion.util.MdcScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Il punto unico da cui passano i log del percorso di ingestione.
 *
 * <p><strong>Cosa faceva prima e perche' e' cambiato.</strong> Anteponeva al messaggio
 * {@code [jobTag=...][runId=...][operationId=...][entity=...][phase=...]}, circa 96 caratteri per
 * riga di contesto che <em>l'encoder JSON emette gia' come attributi</em>: pura duplicazione. Il
 * costo vero non era lo spazio ma l'impossibilita' di aggregare — contenendo il {@code runId},
 * ogni messaggio era unico, quindi un {@code count by message} su Kibana restituiva tante righe
 * quante le esecuzioni invece di dire quante volte una milestone e' avvenuta.
 *
 * <p>Ora il messaggio e' la <strong>sola descrizione della milestone</strong>, come chiede la
 * regola del <em>fatto compiuto</em>, e il contesto va in MDC: da li' l'encoder lo promuove ad
 * attributi di primo livello. I valori variabili si aggiungono con {@link
 * it.pagopa.cruscotto.ingestion.util.LogDetails} e finiscono in {@code ctx_details}.
 *
 * <p><strong>Il contesto viene riscritto a ogni chiamata</strong> anche quando lo scope del job lo
 * ha gia' impostato. E' voluto: rende la riga self-sufficient sui percorsi che non passano da
 * {@code TrackedJobExecutor}, e {@link MdcScope} ripristina i valori precedenti all'uscita, quindi
 * non puo' rubare il contesto a un chiamante.
 *
 * <p>{@code jobTag} e' stato rimosso: era {@code entityName} tradotto in un'altra nomenclatura,
 * cioe' lo stesso dato due volte. Il nome del job resta dove serve davvero, nella colonna
 * {@code JOB_NAME} di {@code INGEST_EXECUTION_LOG}.
 */
public class LogHelper {
    private static final Logger log = LoggerFactory.getLogger(LogHelper.class);

    public static void info(RunContext ctx, RunPhase phase, String message, Object... args) {
        try (MdcScope ignored = scope(ctx, phase.name())) {
            log.info(message, args);
        }
    }

    public static void info(RunContext ctx, String phase, String message, Object... args) {
        try (MdcScope ignored = scope(ctx, phase)) {
            log.info(message, args);
        }
    }

    public static void warn(RunContext ctx, RunPhase phase, String message, Object... args) {
        try (MdcScope ignored = scope(ctx, phase.name())) {
            log.warn(message, args);
        }
    }

    public static void warn(RunContext ctx, String phase, String message, Object... args) {
        try (MdcScope ignored = scope(ctx, phase)) {
            log.warn(message, args);
        }
    }

    public static void debug(RunContext ctx, RunPhase phase, String message, Object... args) {
        try (MdcScope ignored = scope(ctx, phase.name())) {
            log.debug(message, args);
        }
    }

    public static void debug(RunContext ctx, String phase, String message, Object... args) {
        try (MdcScope ignored = scope(ctx, phase)) {
            log.debug(message, args);
        }
    }

    public static void error(RunContext ctx, RunPhase phase, String message, Object... args) {
        try (MdcScope ignored = scope(ctx, phase.name())) {
            log.error(message, args);
        }
    }

    public static void error(RunContext ctx, String phase, String message, Object... args) {
        try (MdcScope ignored = scope(ctx, phase)) {
            log.error(message, args);
        }
    }

    private static MdcScope scope(RunContext ctx, String phase) {
        return MdcScope.open()
                .with(LogFields.CORRELATION_ID, ctx.getRunId())
                .with(LogFields.EVENT_ACTION, ctx.getEntityName())
                .with(LogFields.CTX_PHASE, phase)
                .with(LogFields.CTX_OPERATION_ID, blankToNull(ctx.getOperationId()))
                .with(LogFields.EVENT_OUTCOME, outcomeOf(phase));
    }

    /**
     * L'esito della milestone, dove la fase lo determina.
     *
     * <p>Le fasi non si mappano una a una su {@code event_outcome}: {@code START} e' un'intenzione
     * e non ha esito, {@code WINDOW} e {@code CHECKPOINT} sono passaggi del ciclo. Dichiarare
     * {@code success} su tutte renderebbe il campo inutile per filtrare i fallimenti, che e'
     * l'unica cosa per cui serve. Quindi resta <strong>assente</strong> dove non significa nulla.
     */
    private static String outcomeOf(String phase) {
        if (phase == null) {
            return null;
        }
        return switch (phase) {
            case "END", "BULK_OK" -> "success";
            case "ERROR", "BULK_KO_TOTAL" -> "failure";
            default -> null;
        };
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
