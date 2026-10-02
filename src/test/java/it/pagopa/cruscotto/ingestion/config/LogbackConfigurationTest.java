package it.pagopa.cruscotto.ingestion.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guardia di regressione sulla configurazione logback: non e' coperta dai test applicativi (nessun
 * test carica il context Spring), quindi un errore nei placeholder si manifesta solo a runtime.
 *
 * <p>Caso reale: un {@code ${LOG_DIR}} senza default viene risolto da logback nella stringa
 * letterale {@code LOG_DIR_IS_UNDEFINED}, creando una cartella di log con quel nome.</p>
 */
class LogbackConfigurationTest {

    /** Placeholder logback {@code ${...}} (i pattern {@code %d{...}} non hanno il $ e non matchano). */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");
    private static final Pattern LOCAL_PROPERTY = Pattern.compile("<property\\s+name=\"([^\"]+)\"");
    private static final Pattern LOCAL_PROFILE_BLOCK =
        Pattern.compile("<springProfile\\s+name=\"local\">(.*?)</springProfile>", Pattern.DOTALL);

    private String readConfig() throws IOException {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("logback-spring.xml")) {
            assertThat(in).as("logback-spring.xml deve essere sul classpath").isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void everyPlaceholderHasADefaultOrIsDefinedInTheSameFile() throws IOException {
        String config = readConfig();

        Set<String> localProperties = new LinkedHashSet<>();
        Matcher properties = LOCAL_PROPERTY.matcher(config);
        while (properties.find()) {
            localProperties.add(properties.group(1));
        }

        Set<String> unguarded = new LinkedHashSet<>();
        Set<String> found = new LinkedHashSet<>();
        Matcher placeholders = PLACEHOLDER.matcher(config);
        while (placeholders.find()) {
            String expression = placeholders.group(1);
            found.add(expression);
            boolean hasDefault = expression.contains(":-");
            boolean definedLocally = localProperties.contains(expression);
            if (!hasDefault && !definedLocally) {
                unguarded.add(expression);
            }
        }

        // Il test non deve passare a vuoto: se la regex non intercetta piu' i placeholder la guardia
        // sarebbe inutile anche in presenza di un ${...} non protetto.
        assertThat(found).as("la regex deve intercettare i placeholder della configurazione").isNotEmpty();

        assertThat(unguarded)
            .as("ogni ${...} privo di default viene risolto da logback in <NOME>_IS_UNDEFINED "
                + "(es. la cartella LOG_DIR_IS_UNDEFINED): usare ${nome:-default}")
            .isEmpty();
    }

    /**
     * Il logging su file deve restare confinato al profilo {@code local}: sui pod il filesystem e'
     * effimero e scrivere log su disco consuma spazio senza che i file siano recuperabili.
     */
    @Test
    void fileAppenderIsConfinedToTheLocalProfile() throws IOException {
        String config = readConfig();

        int totalFileAppenders = countOccurrences(config, "RollingFileAppender");
        assertThat(totalFileAppenders)
            .as("il file appender deve esistere (profilo local)")
            .isPositive();

        int insideLocalProfile = 0;
        Matcher blocks = LOCAL_PROFILE_BLOCK.matcher(config);
        while (blocks.find()) {
            insideLocalProfile += countOccurrences(blocks.group(1), "RollingFileAppender");
        }

        assertThat(insideLocalProfile)
            .as("il logging su file non deve essere attivo fuori dal profilo local (pod effimeri)")
            .isEqualTo(totalFileAppenders);
    }

    private int countOccurrences(String text, String token) {
        int count = 0;
        int index = text.indexOf(token);
        while (index >= 0) {
            count++;
            index = text.indexOf(token, index + token.length());
        }
        return count;
    }
}
