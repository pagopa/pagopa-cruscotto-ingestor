package it.pagopa.cruscotto.ingestion.ingestor;

import it.pagopa.cruscotto.ingestion.entity.EntityName;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.time.Instant;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "ingestion")
public class IngestionConfig {
    private Duration initialWindow = Duration.ofMinutes(5);
    private Instant firstRunStart;
    private int maxWindowHalvingAttempts = 10;
    private int bulkInsertSize = 10000;

    @NestedConfigurationProperty
    private GuardrailsConfig guardrails = new GuardrailsConfig();

    @NestedConfigurationProperty
    private AdxConfig adx = new AdxConfig();

    @NestedConfigurationProperty
    private QuartzConfig quartz = new QuartzConfig();

    @NestedConfigurationProperty
    private StagingConfig staging = new StagingConfig();

    @NestedConfigurationProperty
    private AnagraficaConfig anagrafica = new AnagraficaConfig();

    @NestedConfigurationProperty
    private TransformsConfig transforms = new TransformsConfig();

    @NestedConfigurationProperty
    private CheckpointConfig checkpoint = new CheckpointConfig();

    @NestedConfigurationProperty
    private ReconciliationConfig reconciliation = new ReconciliationConfig();

    @NestedConfigurationProperty
    private TokenRegistryCleanupConfig tokenRegistryCleanup = new TokenRegistryCleanupConfig();

    @NestedConfigurationProperty
    private StagingErrorCleanupConfig stagingErrorCleanup = new StagingErrorCleanupConfig();

    @NestedConfigurationProperty
    private EventsWfConfig eventsWf = new EventsWfConfig();

    public Duration getInitialWindow() {
        return initialWindow;
    }

    public Duration getInitialWindow(EntityName entityName) {
        Duration configuredWindow = adx.getWindows().get(entityName);
        return configuredWindow != null ? configuredWindow : initialWindow;
    }

    public Duration resolveWindowForRun(EntityName entityName, Instant cursor, Instant endLimit) {
        Duration realtimeWindow = getInitialWindow(entityName);
        if (!isEventsWfCatchup(entityName, cursor, endLimit)) {
            return realtimeWindow;
        }
        EventsWfConfig.CatchupConfig catchupConfig = eventsWf.getCatchup();
        return positiveOrDefault(catchupConfig.getWindow(), realtimeWindow);
    }

    public Duration resolveMaxDurationForRun(String entityName, boolean catchupMode) {
        Duration defaultMaxDuration = positiveOrDefault(guardrails.getMaxDuration(), Duration.ofMinutes(50));
        EntityName resolvedEntity;
        try {
            resolvedEntity = EntityName.valueOf(entityName);
        } catch (Exception ex) {
            return defaultMaxDuration;
        }

        if (resolvedEntity != EntityName.EVENTS_WF || !catchupMode) {
            return defaultMaxDuration;
        }
        EventsWfConfig.CatchupConfig catchupConfig = eventsWf.getCatchup();
        if (!catchupConfig.isEnabled()) {
            return defaultMaxDuration;
        }
        return positiveOrDefault(catchupConfig.getMaxDuration(), defaultMaxDuration);
    }

    private boolean isEventsWfCatchup(EntityName entityName, Instant cursor, Instant endLimit) {
        if (entityName != EntityName.EVENTS_WF) {
            return false;
        }
        EventsWfConfig.CatchupConfig catchupConfig = eventsWf.getCatchup();
        if (!catchupConfig.isEnabled() || cursor == null || endLimit == null || !cursor.isBefore(endLimit)) {
            return false;
        }
        Duration lag = Duration.between(cursor, endLimit);
        Duration lagThreshold = positiveOrDefault(catchupConfig.getLagThreshold(), Duration.ofHours(2));
        return lag.compareTo(lagThreshold) >= 0;
    }

    public void setInitialWindow(Duration initialWindow) {
        this.initialWindow = initialWindow;
    }

    public Instant getFirstRunStart() {
        return firstRunStart;
    }

    public void setFirstRunStart(Instant firstRunStart) {
        this.firstRunStart = firstRunStart;
    }

    public int getMaxWindowHalvingAttempts() {
        return maxWindowHalvingAttempts;
    }

    public void setMaxWindowHalvingAttempts(int maxWindowHalvingAttempts) {
        this.maxWindowHalvingAttempts = maxWindowHalvingAttempts;
    }

    public int getBulkInsertSize() {
        return bulkInsertSize;
    }

    public void setBulkInsertSize(int bulkInsertSize) {
        this.bulkInsertSize = bulkInsertSize;
    }

    public GuardrailsConfig getGuardrails() {
        return guardrails;
    }

    public void setGuardrails(GuardrailsConfig guardrails) {
        this.guardrails = guardrails;
    }

    public AdxConfig getAdx() {
        return adx;
    }

    public void setAdx(AdxConfig adx) {
        this.adx = adx;
    }

    public QuartzConfig getQuartz() {
        return quartz;
    }

    public void setQuartz(QuartzConfig quartz) {
        this.quartz = quartz;
    }

    public StagingConfig getStaging() {
        return staging;
    }

    public void setStaging(StagingConfig staging) {
        this.staging = staging;
    }

    public AnagraficaConfig getAnagrafica() {
        return anagrafica;
    }

    public void setAnagrafica(AnagraficaConfig anagrafica) {
        this.anagrafica = anagrafica;
    }

    public TransformsConfig getTransforms() {
        return transforms;
    }

    public void setTransforms(TransformsConfig transforms) {
        this.transforms = transforms;
    }

    public CheckpointConfig getCheckpoint() {
        return checkpoint;
    }

    public void setCheckpoint(CheckpointConfig checkpoint) {
        this.checkpoint = checkpoint;
    }

    public ReconciliationConfig getReconciliation() {
        return reconciliation;
    }

    public void setReconciliation(ReconciliationConfig reconciliation) {
        this.reconciliation = reconciliation;
    }

    public TokenRegistryCleanupConfig getTokenRegistryCleanup() {
        return tokenRegistryCleanup;
    }

    public void setTokenRegistryCleanup(TokenRegistryCleanupConfig tokenRegistryCleanup) {
        this.tokenRegistryCleanup = tokenRegistryCleanup;
    }

    public StagingErrorCleanupConfig getStagingErrorCleanup() {
        return stagingErrorCleanup;
    }

    public void setStagingErrorCleanup(StagingErrorCleanupConfig stagingErrorCleanup) {
        this.stagingErrorCleanup = stagingErrorCleanup;
    }

    public EventsWfConfig getEventsWf() {
        return eventsWf;
    }

    public void setEventsWf(EventsWfConfig eventsWf) {
        this.eventsWf = eventsWf;
    }

    // ---------------------------------------------------------------
    // Nested config classes
    // ---------------------------------------------------------------

    public static class AnagraficaConfig {
        @NestedConfigurationProperty
        private CacheConfig cache = new CacheConfig();

        public CacheConfig getCache() {
            return cache;
        }

        public void setCache(CacheConfig cache) {
            this.cache = cache;
        }

        public static class CacheConfig {
            /** Abilita la cache in-memory. */
            private boolean enabled = true;
            /** TTL della cache in minuti. */
            private int ttlMinutes = 60;
            /** Dimensione massima della cache per tipo. */
            private int maxSize = 10000;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public int getTtlMinutes() {
                return ttlMinutes;
            }

            public void setTtlMinutes(int ttlMinutes) {
                this.ttlMinutes = ttlMinutes;
            }

            public int getMaxSize() {
                return maxSize;
            }

            public void setMaxSize(int maxSize) {
                this.maxSize = maxSize;
            }
        }
    }

    public static class StagingConfig {
        private boolean enabled = true;
        /** Numero massimo di retry per un record in STG_INGEST_ERROR prima di marcarlo PARKED. */
        private int maxRetries = 5;
        /**
         * Dopo quanto tempo un record PARKED viene rimesso in PENDING per un nuovo ciclo di retry.
         * Questo garantisce che nessuna riga ADX venga persa definitivamente: quando l'entità padre
         * raggiunge il timestamp del record, il ciclo di retry successivo riuscirà.
         * Default: 30 minuti.
         */
        private Duration unparkAfter = Duration.ofMinutes(30);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public Duration getUnparkAfter() {
            return unparkAfter;
        }

        public void setUnparkAfter(Duration unparkAfter) {
            this.unparkAfter = unparkAfter;
        }
    }

    public static class TransformsConfig {
        @NestedConfigurationProperty
        private PositionConfig position = new PositionConfig();

        public PositionConfig getPosition() {
            return position;
        }

        public void setPosition(PositionConfig position) {
            this.position = position;
        }

        public static class PositionConfig {
            /** Finestra temporale in ore per la deduplicazione POSITION (NAV + PA_EMITTENTE). */
            private int windowHours = 24;

            public int getWindowHours() {
                return windowHours;
            }

            public void setWindowHours(int windowHours) {
                this.windowHours = windowHours;
            }
        }
    }

    public static class CheckpointConfig {
        /** Se true, aggiorna il checkpoint solo in caso di bulk insert completato con successo. */
        private boolean updateOnlyOnSuccess = true;

        public boolean isUpdateOnlyOnSuccess() {
            return updateOnlyOnSuccess;
        }

        public void setUpdateOnlyOnSuccess(boolean updateOnlyOnSuccess) {
            this.updateOnlyOnSuccess = updateOnlyOnSuccess;
        }
    }

    public static class ReconciliationConfig {
        private boolean enabled = true;
        /** Numero massimo di record da processare per ciclo di reconciliation. */
        private int batchSize = 500;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }
    }

    public static class TokenRegistryCleanupConfig {
        private boolean enabled = true;
        private Duration retention = Duration.ofDays(7);
        private String cron = "0 30 2 * * ?";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getRetention() {
            return retention;
        }

        public void setRetention(Duration retention) {
            this.retention = retention;
        }

        public String getCron() {
            return cron;
        }

        public void setCron(String cron) {
            this.cron = cron;
        }
    }

    public static class StagingErrorCleanupConfig {
        private boolean enabled = true;
        private Duration retention = Duration.ofDays(7);
        private String cron = "0 45 2 * * ?";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getRetention() {
            return retention;
        }

        public void setRetention(Duration retention) {
            this.retention = retention;
        }

        public String getCron() {
            return cron;
        }

        public void setCron(String cron) {
            this.cron = cron;
        }
    }

    public static class EventsWfConfig {
        @NestedConfigurationProperty
        private CatchupConfig catchup = new CatchupConfig();

        @NestedConfigurationProperty
        private DedicatedConfig dedicated = new DedicatedConfig();

        public CatchupConfig getCatchup() {
            return catchup;
        }

        public void setCatchup(CatchupConfig catchup) {
            this.catchup = catchup;
        }

        public DedicatedConfig getDedicated() {
            return dedicated;
        }

        public void setDedicated(DedicatedConfig dedicated) {
            this.dedicated = dedicated;
        }

        public static class CatchupConfig {
            private boolean enabled = true;
            private Duration lagThreshold = Duration.ofHours(2);
            private Duration window = Duration.ofMinutes(15);
            private Duration maxDuration;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public Duration getLagThreshold() {
                return lagThreshold;
            }

            public void setLagThreshold(Duration lagThreshold) {
                this.lagThreshold = lagThreshold;
            }

            public Duration getWindow() {
                return window;
            }

            public void setWindow(Duration window) {
                this.window = window;
            }

            public Duration getMaxDuration() {
                return maxDuration;
            }

            public void setMaxDuration(Duration maxDuration) {
                this.maxDuration = maxDuration;
            }
        }

        public static class DedicatedConfig {
            private boolean enabled = true;
            private int extraRunsWhenBacklogged = 2;
            private Duration backlogThreshold = Duration.ofHours(2);

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public int getExtraRunsWhenBacklogged() {
                return extraRunsWhenBacklogged;
            }

            public void setExtraRunsWhenBacklogged(int extraRunsWhenBacklogged) {
                this.extraRunsWhenBacklogged = extraRunsWhenBacklogged;
            }

            public Duration getBacklogThreshold() {
                return backlogThreshold;
            }

            public void setBacklogThreshold(Duration backlogThreshold) {
                this.backlogThreshold = backlogThreshold;
            }
        }
    }

    public static class GuardrailsConfig {
        private boolean enableMaxDuration = true;
        private Duration maxDuration = Duration.ofMinutes(50);
        private boolean enableMaxQueries = false;
        private int maxQueries = 120;
        private boolean enableMaxRows = false;
        private int maxRows = 2_000_000;

        public boolean isEnableMaxDuration() {
            return enableMaxDuration;
        }

        public void setEnableMaxDuration(boolean enableMaxDuration) {
            this.enableMaxDuration = enableMaxDuration;
        }

        public Duration getMaxDuration() {
            return maxDuration;
        }

        public void setMaxDuration(Duration maxDuration) {
            this.maxDuration = maxDuration;
        }

        public boolean isEnableMaxQueries() {
            return enableMaxQueries;
        }

        public void setEnableMaxQueries(boolean enableMaxQueries) {
            this.enableMaxQueries = enableMaxQueries;
        }

        public int getMaxQueries() {
            return maxQueries;
        }

        public void setMaxQueries(int maxQueries) {
            this.maxQueries = maxQueries;
        }

        public boolean isEnableMaxRows() {
            return enableMaxRows;
        }

        public void setEnableMaxRows(boolean enableMaxRows) {
            this.enableMaxRows = enableMaxRows;
        }

        public int getMaxRows() {
            return maxRows;
        }

        public void setMaxRows(int maxRows) {
            this.maxRows = maxRows;
        }
    }

    public static class AdxConfig {
        private int maxResultSizeMb = 64;
        private Duration queryTimeout;
        private String endpoint;
        private String database = "re";
        private boolean includeEstimates = false;
        private Map<EntityName, Duration> windows = new EnumMap<>(EntityName.class);

        public int getMaxResultSizeMb() {
            return maxResultSizeMb;
        }

        public void setMaxResultSizeMb(int maxResultSizeMb) {
            this.maxResultSizeMb = maxResultSizeMb;
        }

        public Duration getQueryTimeout() {
            return queryTimeout;
        }

        public void setQueryTimeout(Duration queryTimeout) {
            this.queryTimeout = queryTimeout;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(String endpoint) {
            this.endpoint = endpoint;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public boolean isIncludeEstimates() {
            return includeEstimates;
        }

        public void setIncludeEstimates(boolean includeEstimates) {
            this.includeEstimates = includeEstimates;
        }

        public Map<EntityName, Duration> getWindows() {
            return windows;
        }

        public void setWindows(Map<EntityName, Duration> windows) {
            this.windows = (windows == null || windows.isEmpty())
                    ? new EnumMap<>(EntityName.class)
                    : new EnumMap<>(windows);
        }
    }

    public static class QuartzConfig {
        private boolean enabled = true;
        private int threadCount = 3;
        private Map<EntityName, JobCronConfig> jobs = new EnumMap<>(EntityName.class);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getThreadCount() {
            return threadCount;
        }

        public void setThreadCount(int threadCount) {
            this.threadCount = threadCount;
        }


        public Map<EntityName, JobCronConfig> getJobs() {
            return jobs;
        }

        public void setJobs(Map<EntityName, JobCronConfig> jobs) {
            this.jobs = jobs;
        }
    }

    public static class JobCronConfig {
        private String cron;
        private boolean enabled = true;

        public String getCron() {
            return cron;
        }

        public void setCron(String cron) {
            this.cron = cron;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    private Duration positiveOrDefault(Duration candidate, Duration fallback) {
        if (candidate == null || candidate.isNegative() || candidate.isZero()) {
            return fallback;
        }
        return candidate;
    }
}
