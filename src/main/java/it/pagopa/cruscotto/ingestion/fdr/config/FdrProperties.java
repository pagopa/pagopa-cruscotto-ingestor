package it.pagopa.cruscotto.ingestion.fdr.config;

import it.pagopa.cruscotto.ingestion.util.LogDetails;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configuration properties for the FDR (Flussi di Rendicontazione) bounded context.
 *
 * <p>Bound from the {@code fdr:} section of {@code application.yml}, kept fully separate from the
 * ADX ingestion configuration ({@code ingestion.*}) and from the Massive Search one
 * ({@code massive-search.*}), following the same convention: no container name, path or batch size
 * is hardcoded in business logic.</p>
 *
 * <p><strong>Disabled by default.</strong> Unlike {@code massive-search.enabled}, which defaults to
 * {@code true} because the feature is live, FDR defaults to {@code false}: a deployment that knows
 * nothing about {@code fdr.*} must start and behave exactly as before. The flag is the only reason
 * this class can exist before the feature does.</p>
 *
 * <p>This is <em>only</em> a configuration model: it holds no collaborator, opens no connection and
 * registers no job. Nothing reads it yet.</p>
 */
@Slf4j
@Configuration
@ConfigurationProperties(prefix = "fdr")
@Getter
@Setter
public class FdrProperties {

    /** Master switch for the FDR feature. Off unless an environment opts in. */
    private boolean enabled = false;

    @NestedConfigurationProperty
    private Storage storage = new Storage();

    @NestedConfigurationProperty
    private Processing processing = new Processing();

    @NestedConfigurationProperty
    private Retry retry = new Retry();

    @NestedConfigurationProperty
    private Daily daily = new Daily();

    @NestedConfigurationProperty
    private Command command = new Command();

    @NestedConfigurationProperty
    private Recovery recovery = new Recovery();

    /**
     * Blob container of the daily FDR drops. The storage account itself (connection string /
     * secret) is shared and lives under {@code azure.blob}: only the dedicated container is defined
     * here, so no secret is duplicated. This mirrors what Massive Search already does — and it is
     * literally the same storage account.
     *
     * <p>There is no base-path knob. The layout is confirmed: the daily folders
     * ({@code yyyyMMdd/}) sit at the container root, so there is no static prefix to configure.
     * The day is dynamic and belongs to the path builder, not to configuration.</p>
     */
    @Getter
    @Setter
    public static class Storage {

        /**
         * Container holding the daily FDR drops, on the shared storage account.
         *
         * <p>The name is now a confirmed infrastructure fact, so it carries a default, exactly like
         * {@code massive-search.storage.container} does. It had none while it was still unknown: a
         * plausible-looking default would have let the application start pointing at a container
         * that does not exist, and the failure would have surfaced much later, on the first read.
         * That reason has expired.</p>
         *
         * <p>Still overridable per environment, and still validated when the feature is on — the
         * default protects against a missing value, not against one explicitly blanked out.</p>
         */
        private String container = "fdr";
    }

    /** Processing tuning for the day pipeline. */
    @Getter
    @Setter
    public static class Processing {

        /**
         * Rows accumulated before a write is flushed to PostgreSQL.
         *
         * <p>The default is not a new figure: the ingestion path already works in blocks of this
         * order of magnitude ({@code ingestion.reconciliation.batch-size} is 1000, the Hibernate JDBC
         * batch is 2000, the execution-log and metadata cleanups use 500). 1000 sits inside that
         * range. It matters because FDR archives reach ~400 MB and must be processed as a stream:
         * the batch size is what keeps the footprint bounded regardless of file size.</p>
         */
        private int batchSize = 1000;
    }

    /**
     * Technical retry of a whole day, as confirmed by the customer: at most 5 attempts, one hour
     * apart.
     *
     * <p>The values live here, the behaviour does not exist yet. The retry needs FDR-specific
     * persisted state — a day is not representable in {@code INGEST_EXECUTION_LOG}, and
     * {@code STG_INGEST_ERROR} is per row — so it is deliberately left to a later step. These two
     * knobs are declared now only so the figures have a single home when that step arrives.</p>
     */
    @Getter
    @Setter
    public static class Retry {

        /** Maximum number of attempts for a single day, the first one included. */
        private int maxAttempts = 5;

        /** Delay between two attempts on the same day. */
        private Duration interval = Duration.ofHours(1);

        /** Whether the retry poller is scheduled at all. */
        private boolean pollEnabled = false;

        /**
         * How often the poller looks for days whose attempt has come due.
         *
         * <p>Distinct from {@link #interval}, and it must be: polling every five minutes does not
         * mean retrying every five minutes. The poller only picks days whose
         * {@code NEXT_ATTEMPT_AT} has passed, so a short poll makes the retry <em>punctual</em>,
         * not frequent.
         */
        private String pollCron = "0 */5 * * * ?";
    }

    /**
     * The daily run: the only workflow the Ingestor starts on its own.
     *
     * <p>In production it fires at 05:00 on yesterday's folder. The time zone is explicit because
     * an implicit one would follow the JVM's, and a container that changes base image can change
     * it without anyone noticing — moving the run by an hour twice a year.
     */
    @Getter
    @Setter
    public static class Daily {

        private boolean enabled = false;

        /** 05:00 every day. Fully configurable so that DEV can fire every couple of minutes. */
        private String cron = "0 0 5 * * ?";

        private String timezone = "Europe/Rome";

        /**
         * Forces the business day instead of using yesterday. <strong>DEV only.</strong>
         *
         * <p>It exists so a historical day can be exercised on demand without waiting for 05:00 or
         * for the date to change. In production it must stay empty: a fixed day here would mean
         * reprocessing the same folder forever and never ingesting the current one.
         */
        private String overrideBusinessDay;
    }

    /**
     * The poller of the administrative requests the backend writes to the database.
     *
     * <p>There is no HTTP endpoint on the Ingestor by design: the backend records what it wants and
     * the Ingestor picks it up. The two never have to be up at the same time.
     */
    @Getter
    @Setter
    public static class Command {

        private boolean enabled = false;

        /** Every minute. A request waits at most one poll before being taken. */
        private String cron = "0 * * * * ?";
    }

    /**
     * Liveness del worker che sta lavorando: quanto vale il possesso e ogni quanto si rinnova.
     *
     * <p><strong>Non confondere questi due con {@code fdr.retry.interval}.</strong> Sono concetti
     * diversi e vanno tarati in modo indipendente: lease e battito dicono se un worker è vivo
     * <em>adesso</em>, l'intervallo di retry dice quanto aspettare prima di ritentare una giornata
     * che è fallita per un guasto tecnico. Il primo si misura in minuti, il secondo in ore.
     */
    @Getter
    @Setter
    public static class Recovery {

        /**
         * Da quanto una <em>richiesta</em> deve essere ferma in {@code PROCESSING} prima di
         * considerarla abbandonata.
         *
         * <p>Resta una soglia di tempo perché riguarda {@code FDR_INGESTION_BATCH}, che non ha un
         * lease: il lease sta sulle giornate, che sono l'unità di lavoro. La soglia però non decide
         * da sola — si rimette in coda una richiesta solo se <em>nessuna</em> delle sue giornate ha
         * un lease ancora valido, cioè se davvero non ci sta lavorando nessuno.
         */
        private Duration staleAfter = Duration.ofMinutes(15);

        /**
         * Quanto dura il possesso di una giornata senza essere rinnovato.
         *
         * <p>È il ritardo massimo con cui una giornata abbandonata viene ripresa, quindi tenerlo
         * basso accorcia il disservizio; ma è anche la pazienza verso un worker lento, quindi
         * tenerlo troppo basso fa subentrare un secondo worker su una giornata ancora viva. Cinque
         * minuti stanno comodamente sopra qualunque pausa normale fra due batch e ben sotto il
         * tempo di una giornata intera.
         */
        private Duration leaseDuration = Duration.ofMinutes(5);

        /**
         * Ogni quanto rinnovare il possesso nelle fasi che non scrivono dato business.
         *
         * <p>Dentro una transazione di scrittura il rinnovo avviene <em>sempre</em>, perché lì è
         * anche la verifica del diritto di scrivere e non è saltabile. Questo intervallo governa
         * solo le fasi senza transazione — discovery, preflight, passaggio da un file al successivo
         * — dove rinnovare a ogni occasione sarebbe una scrittura ogni pochi millisecondi senza
         * alcun guadagno.
         */
        private Duration heartbeatInterval = Duration.ofSeconds(30);
    }

    /**
     * Fails fast at startup when the feature is on and its configuration cannot work.
     *
     * <p>Validation is gated on {@link #enabled} on purpose: the existing environments do not supply
     * any {@code fdr.*} key and must keep starting. Checking unconditionally would turn an additive
     * change into a breaking one.</p>
     *
     * <p>Done with {@code @PostConstruct} rather than {@code @Validated} + constraint annotations
     * because bean validation is not on the runtime classpath — {@code jakarta.validation-api} is
     * declared only among the Liquibase plugin dependencies. This is the same idiom
     * {@code AdxTableNamesConfig} already uses.</p>
     *
     * @throws IllegalStateException when the feature is enabled but misconfigured
     */
    @PostConstruct
    void validateConfiguration() {
        if (!enabled) {
            log.debug("Skipped FDR configuration validation, the feature is disabled");
            return;
        }
        requireText(storage.getContainer(), "fdr.storage.container");
        requireNoWhitespace(storage.getContainer(), "fdr.storage.container");
        requirePositive(processing.getBatchSize(), "fdr.processing.batch-size");
        requirePositive(retry.getMaxAttempts(), "fdr.retry.max-attempts");
        requirePositiveDuration(retry.getInterval(), "fdr.retry.interval");
        requirePositiveDuration(recovery.getStaleAfter(), "fdr.recovery.stale-after");
        requirePositiveDuration(recovery.getLeaseDuration(), "fdr.recovery.lease-duration");
        requirePositiveDuration(recovery.getHeartbeatInterval(), "fdr.recovery.heartbeat-interval");
        validateLease();
        validateSchedules();

        try (LogDetails ignored = LogDetails.open()
                .with("container", storage.getContainer())
                .with("batch_size", processing.getBatchSize())
                .with("max_attempts", retry.getMaxAttempts())
                .with("retry_interval_ms", retry.getInterval().toMillis())) {
            log.info("Validated FDR configuration");
        }
    }

    /**
     * Checks what the scheduler would otherwise discover much later, or never.
     *
     * <p>The three failures guarded here are the ones that stay quiet. A malformed cron only
     * surfaces when Quartz parses the trigger, which happens once, at startup, in a log line nobody
     * reads; an unknown time zone does not fail at all — {@code TimeZone.getTimeZone} answers GMT,
     * so the daily run would silently move by an hour or two; and a stale
     * {@code override-business-day} left over in production would keep reprocessing the same folder
     * forever while the current days are never ingested.</p>
     */
    /**
     * Il lease deve sopravvivere a un battito mancato.
     *
     * <p>Il margine richiesto è il <strong>doppio</strong>, e non semplicemente "maggiore".
     * Con lease e battito vicini, un singolo rinnovo ritardato — una pausa del garbage collector,
     * una query lenta, un secondo di rete — farebbe scadere il possesso mentre il worker sta
     * lavorando benissimo. Un altro subentrerebbe, e il primo scoprirebbe di aver perso la giornata
     * al tentativo di scrittura successivo: nessun dato sbagliato, ma lavoro buttato e una giornata
     * che rimbalza fra pod. Col doppio, perdere il possesso richiede che ne saltino due di fila.
     *
     * <p>L'errore cita i due valori perché è una relazione fra loro: sapere solo quale delle due
     * property è "sbagliata" non basta a decidere quale cambiare.
     */
    private void validateLease() {
        Duration lease = recovery.getLeaseDuration();
        Duration heartbeat = recovery.getHeartbeatInterval();
        if (lease.compareTo(heartbeat.multipliedBy(2)) < 0) {
            throw new IllegalStateException(
                    "fdr.recovery.lease-duration (" + lease + ") must be at least twice "
                            + "fdr.recovery.heartbeat-interval (" + heartbeat + "), so that a "
                            + "single missed renewal does not lose the day");
        }
    }

    private void validateSchedules() {
        if (daily.isEnabled()) {
            requireCron(daily.getCron(), "fdr.daily.cron");
            requireKnownTimezone(daily.getTimezone(), "fdr.daily.timezone");
        }
        if (command.isEnabled()) {
            requireCron(command.getCron(), "fdr.command.cron");
        }
        if (retry.isPollEnabled()) {
            requireCron(retry.getPollCron(), "fdr.retry.poll-cron");
        }
        requireIsoDate(daily.getOverrideBusinessDay(), "fdr.daily.override-business-day");
    }

    private static void requireCron(String value, String property) {
        requireText(value, property);
        if (!org.quartz.CronExpression.isValidExpression(value)) {
            throw new IllegalStateException(
                    property + " is not a valid cron expression, was \"" + value + "\"");
        }
    }

    /**
     * Rejects a time zone Java does not know.
     *
     * <p>{@code TimeZone.getTimeZone} never throws: an unrecognised id yields GMT. Left unchecked,
     * a typo in the zone would move the daily run by the local offset and nothing would report it.
     */
    private static void requireKnownTimezone(String value, String property) {
        requireText(value, property);
        try {
            java.time.ZoneId.of(value.trim());
        } catch (java.time.DateTimeException e) {
            throw new IllegalStateException(
                    property + " is not a known time zone, was \"" + value + "\"", e);
        }
    }

    /** Empty is the normal case: the override only exists for DEV. */
    private static void requireIsoDate(String value, String property) {
        if (value == null || value.isBlank()) {
            return;
        }
        try {
            java.time.LocalDate.parse(value.trim());
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalStateException(
                    property + " must be an ISO date (yyyy-MM-dd), was \"" + value + "\"", e);
        }
    }

    private static void requireText(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(property + " is required when fdr.enabled is true");
        }
    }

    /**
     * Rejects a container name carrying whitespace.
     *
     * <p>Worth a check of its own because the failure it prevents is invisible: a stray space looks
     * identical to a correct value in every listing and in every log. It survives the blank check,
     * reaches Azure, and comes back as a container-not-found at the first read — far from its
     * cause. Azure container names cannot contain whitespace at all, so there is no legitimate case
     * to preserve.</p>
     *
     * <p>The realistic source is an environment variable or a ConfigMap entry, where nothing
     * normalises the value: both the YAML parser and Spring's inline test properties trim it away,
     * so those two paths cannot produce it.</p>
     *
     * <p>The message quotes the value on purpose: it is the only way the offending character
     * becomes visible to whoever reads the error.</p>
     */
    private static void requireNoWhitespace(String value, String property) {
        if (value.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalStateException(
                    property + " must not contain whitespace, was \"" + value + "\"");
        }
    }

    private static void requirePositive(int value, String property) {
        if (value <= 0) {
            throw new IllegalStateException(property + " must be greater than zero, was " + value);
        }
    }

    private static void requirePositiveDuration(Duration value, String property) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalStateException(property + " must be a positive duration, was " + value);
        }
    }
}
