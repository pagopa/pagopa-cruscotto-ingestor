package it.pagopa.cruscotto.ingestion.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties specific to PagoPa Cruscotto Backend.
 * <p>
 * Properties are configured in the {@code application.yml} file.
 */
@Getter
@ConfigurationProperties(prefix = "application", ignoreUnknownFields = true)
public class ApplicationProperties {

    private final Liquibase liquibase = new Liquibase();

    @Setter
    @Getter
    public static class Liquibase {

        private Boolean asyncStart = true;

        private String dbVersion;
    }


}
