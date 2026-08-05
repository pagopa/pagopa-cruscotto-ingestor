package it.pagopa.cruscotto.ingestion.massivesearch.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Configuration properties for the Massive Search (Ricerca Massiva) bounded context.
 *
 * <p>Bound from the {@code massive-search:} section of {@code application.yml}. Kept fully
 * separate from the ADX ingestion configuration ({@code ingestion.*}). No file names, paths,
 * container names or batch sizes are hardcoded in business logic: they are resolved here.</p>
 */
@Configuration
@ConfigurationProperties(prefix = "massive-search")
@Getter
@Setter
public class MassiveSearchProperties {

    /** Master switch for the Massive Search feature. */
    private boolean enabled = true;

    @NestedConfigurationProperty
    private Storage storage = new Storage();

    @NestedConfigurationProperty
    private Csv csv = new Csv();

    @NestedConfigurationProperty
    private Reports reports = new Reports();

    @NestedConfigurationProperty
    private Execution execution = new Execution();

    @NestedConfigurationProperty
    private Perimeter perimeter = new Perimeter();

    @NestedConfigurationProperty
    private Naming naming = new Naming();

    @NestedConfigurationProperty
    private Scheduler scheduler = new Scheduler();

    /**
     * Blob container and base path for perimeter and result artifacts. The storage account itself
     * (connection string / secret) is shared and lives under {@code azure.blob}; only the dedicated
     * container and base path are defined here, so no secret is duplicated.
     */
    @Getter
    @Setter
    public static class Storage {
        private String type = "local";
        private String container = "massive-search";
        private String basePath = "massive-search";
        private String perimeterPath = "instances";
        private String executionPath = "executions";
        private String localRootDir;

        /** Logical relative path of an artifact belonging to an execution. */
        public String executionObjectPath(Object executionId, String fileName) {
            return executionPath + "/" + executionId + "/" + fileName;
        }

        /** Logical relative path of an artifact belonging to an instance (e.g. the perimeter). */
        public String perimeterObjectPath(Object instanceId, String fileName) {
            return perimeterPath + "/" + instanceId + "/" + fileName;
        }
    }

    /** CSV read/write settings shared by perimeter and report generation. */
    @Getter
    @Setter
    public static class Csv {
        private String separator = ",";
        private Charset charset = StandardCharsets.UTF_8;
        private int maxRows = 500000;
        private int maxValidationErrors = 1000;
    }

    /** File names for the three per-execution reports and the resulting ZIP. */
    @Getter
    @Setter
    public static class Reports {
        private String positionFileName = "posizioni.csv";
        private String attemptFileName = "tentativi.csv";
        private String transferFileName = "versamenti.csv";
    }

    /** Execution engine tuning. */
    @Getter
    @Setter
    public static class Execution {
        private boolean allowConcurrentExecutionPerInstance = false;
        private int pageSize = 10000;
        /**
         * Number of perimeter input keys grouped into a single set-based report query. Reading the
         * perimeter in blocks turns the former one-query-per-key pattern into {@code ceil(N/batch)}
         * queries per report, drastically cutting DB round-trips on large perimeters while keeping
         * memory bounded. Must be &gt;= 1.
         */
        private int perimeterBatchSize = 500;
        /**
         * An execution still {@code RUNNING} after this many minutes from {@code started_at} is
         * considered stuck (e.g. the pod was killed) and recovered to {@code FAILED} by the scanner.
         */
        private int runningTimeoutMinutes = 60;
    }

    /** Perimeter CSV settings (filter-driven searches). */
    @Getter
    @Setter
    public static class Perimeter {
        private String generatedTemplate = "NAV_PA";
    }

    /**
     * Naming convention for the deliverable artifacts (downloadable ZIP and perimeter CSV).
     *
     * <p>The physical storage layout already disambiguates artifacts by per-id folders
     * ({@code instances/&lt;instanceId&gt;/}, {@code executions/&lt;executionId&gt;/}); this convention makes the
     * <em>file names themselves</em> self-describing and traceable once downloaded, following the pattern
     * {@code <prefix><separator><shortId><separator><timestamp><extension>}, e.g.
     * {@code ricerca-massiva__a1b2c3d4__20260804-153500.zip}. The three report CSVs kept inside the ZIP
     * retain their human-readable names ({@code posizioni.csv}, {@code tentativi.csv}, {@code versamenti.csv}).</p>
     */
    @Getter
    @Setter
    public static class Naming {
        /** Token separator inserted between prefix, short id and timestamp. */
        private String separator = "__";
        /** {@link java.time.format.DateTimeFormatter} pattern for the timestamp token. */
        private String timestampPattern = "yyyyMMdd-HHmmss";
        /** Zone the timestamp is rendered in (defaults to UTC for reproducibility across pods). */
        private String timestampZone = "UTC";
        /** Number of leading hex characters of the correlation UUID used as short id. */
        private int shortIdLength = 8;
        /** Prefix of the downloadable result archive. */
        private String zipPrefix = "ricerca-massiva";
        /** Extension of the downloadable result archive. */
        private String zipExtension = ".zip";
        /** Prefix of the generated perimeter CSV. */
        private String perimeterPrefix = "perimetro";
        /** Extension of the generated perimeter CSV. */
        private String perimeterExtension = ".csv";
    }

    /**
     * Quartz scanner settings. The scanner polls {@code SEARCH_INSTANCE} for {@code READY} instances
     * and submits them to the dedicated task executor; it never runs the long analysis on the Quartz
     * thread. All values are configurable (no hardcoding).
     */
    @Getter
    @Setter
    public static class Scheduler {
        private boolean enabled = true;
        private String cron = "0 */1 * * * ?";
        private int maxInstancesPerRun = 10;
        private int maxConcurrentExecutions = 3;
    }
}
