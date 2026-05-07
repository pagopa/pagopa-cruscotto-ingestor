package it.pagopa.cruscotto.ingestion.ingestor;

import it.pagopa.cruscotto.ingestion.entity.EntityName;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "ingestion")
public class IngestionConfig {
    private Duration initialWindow = Duration.ofMinutes(5);
    private int maxWindowHalvingAttempts = 10;
    private int bulkInsertSize = 5000;
    private int reconciliationFetchLimit = 200;

    @NestedConfigurationProperty
    private GuardrailsConfig guardrails = new GuardrailsConfig();

    @NestedConfigurationProperty
    private AdxConfig adx = new AdxConfig();

    @NestedConfigurationProperty
    private QuartzConfig quartz = new QuartzConfig();

    public Duration getInitialWindow() {
        return initialWindow;
    }

    public void setInitialWindow(Duration initialWindow) {
        this.initialWindow = initialWindow;
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

    public int getReconciliationFetchLimit() {
        return reconciliationFetchLimit;
    }

    public void setReconciliationFetchLimit(int reconciliationFetchLimit) {
        this.reconciliationFetchLimit = reconciliationFetchLimit;
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
    }

    public static class QuartzConfig {
        private Map<EntityName, JobCronConfig> jobs = new EnumMap<>(EntityName.class);

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
}
