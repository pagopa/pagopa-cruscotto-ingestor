package it.pagopa.cruscotto.ingestion;

import it.pagopa.cruscotto.ingestion.configuration.ApplicationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({ LiquibaseProperties.class, ApplicationProperties.class })
public class IngestorApplicationApp {

	public static void main(String[] args) {
		SpringApplication.run(IngestorApplicationApp.class, args);
	}

}
