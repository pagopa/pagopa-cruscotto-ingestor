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

        try (LogDetails ignored = LogDetails.open()
                .with("container", storage.getContainer())
                .with("batch_size", processing.getBatchSize())
                .with("max_attempts", retry.getMaxAttempts())
                .with("retry_interval_ms", retry.getInterval().toMillis())) {
            log.info("Validated FDR configuration");
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
