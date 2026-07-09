package it.pagopa.cruscotto.ingestion.config;

import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.servers.ServerVariable;
import io.swagger.v3.oas.models.servers.ServerVariables;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class OpenApiConfiguration {

    @Bean
    public OpenApiCustomizer pagopaServerOpenApiCustomizer() {
        return openApi -> {
            Server server = new Server();
            server.setUrl("https://{host}");
            server.setDescription("Ambienti PagoPA (oltre a localhost)");

            ServerVariable hostVariable = new ServerVariable();
            hostVariable.setDefault("api.dev.platform.pagopa.it");
            hostVariable.setEnum(Arrays.asList(
                "api.dev.platform.pagopa.it",
                "api.uat.platform.pagopa.it",
                "api.platform.pagopa.it"
            ));

            ServerVariables variables = new ServerVariables();
            variables.addServerVariable("host", hostVariable);

            server.setVariables(variables);

            openApi.addServersItem(server);
        };
    }




    @Bean
    public GroupedOpenApi actuatorApi() {
        return GroupedOpenApi.builder()
            .group("management")
            .pathsToMatch(
                "/management/**"
            )
            .pathsToExclude(
                "/management/env/**",
                "/management/configprops/**",
                "/management/loggers/**",
                "/management/logfile/**",
                "/management/threaddump/**",
                "/management/caches/**",
                "/management/jhimetrics/**",
                "/management/jhiopenapigroups/**",
                ("/api/**")

            )

            .build();
    }
}
