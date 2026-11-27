package it.pagopa.cruscotto.ingestion.configuration;

import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Le connessioni verso PostgreSQL devono dire chi le ha aperte.
 *
 * <h2>Perché esiste</h2>
 *
 * <p>Senza {@code ApplicationName} il driver si presenta come {@code "PostgreSQL JDBC Driver"}, e
 * su un database condiviso fra ingestor e backend {@code pg_stat_activity} diventa un elenco di
 * righe indistinguibili. È costato un giro di deduzioni durante un'analisi di lock su dev — l'età
 * della transazione confrontata col nostro {@code idle_in_transaction_session_timeout}, lo schema
 * nel {@code search_path} — per arrivare a una conclusione che la colonna avrebbe dato subito.
 *
 * <h2>Perché il test guarda il case, e non solo il valore</h2>
 *
 * <p>PgJDBC legge {@code "ApplicationName"} con il case esatto e <strong>ignora in silenzio</strong>
 * una chiave normalizzata: nessun errore all'avvio, nessuna riga di log, e la colonna in
 * {@code pg_stat_activity} resta al valore di default. È un difetto che non si vede da nessuna
 * parte se non collegandosi al database, quindi vale la pena ancorarlo.
 *
 * <p>Si lega il file vero su un {@link HikariConfig} vero e si guarda cosa arriverebbe al driver.
 * Un test che si limitasse a cercare la stringa nel YAML passerebbe anche nel caso rotto, perché
 * la differenza la fa la conversione di Spring e non il testo del file.
 */
class DatasourceApplicationNameTest {

    /** Il nome esatto della property del driver. Case-sensitive: vedi {@code PGProperty}. */
    private static final String PG_APPLICATION_NAME = "ApplicationName";

    private Properties boundDataSourceProperties() throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        // Le property source di sistema vengono rimosse di proposito: un SPRING_DATASOURCE_*
        // impostato sulla macchina di chi esegue i test cambierebbe il risultato, e un test che
        // dipende dall'ambiente e' peggio di nessun test.
        environment.getPropertySources()
                .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources()
                .remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"))
                .forEach(source -> environment.getPropertySources().addFirst(source));

        // Si lega su un HikariConfig vero, non su una Map: cosi' il test percorre la stessa
        // conversione che fa Spring Boot all'avvio, parentesi quadre comprese.
        return Binder.get(environment)
                .bind("spring.datasource.hikari", HikariConfig.class)
                .orElseThrow(() -> new AssertionError(
                        "la sezione spring.datasource.hikari non si lega a HikariConfig"))
                .getDataSourceProperties();
    }

    @Test
    void leConnessioniSiPresentanoConUnNomeApplicativo() throws IOException {
        Properties properties = boundDataSourceProperties();

        assertThat(properties.getProperty(PG_APPLICATION_NAME))
                .as("senza questa property pg_stat_activity mostra 'PostgreSQL JDBC Driver' per "
                        + "ingestor e backend insieme, e i lock non sono attribuibili")
                .isNotBlank();
    }

    /**
     * Il case della chiave deve sopravvivere al binding di Spring.
     *
     * <p>Verificato per mutazione che oggi il case si conserva <em>anche</em> senza la notazione a
     * parentesi nel YAML: non è quindi quella a salvarci, ed è bene saperlo invece di crederlo.
     * Il test resta perché il vincolo è del driver e non di Spring — chiunque "normalizzasse" la
     * chiave, nel file o in una property source di un ambiente, romperebbe la firma delle
     * connessioni senza che nulla lo segnali.
     */
    @Test
    void laChiaveConservaIlCaseCheIlDriverSiAspetta() throws IOException {
        Properties properties = boundDataSourceProperties();

        assertThat(properties.stringPropertyNames())
                .as("PgJDBC legge la property con il case esatto: una chiave normalizzata a "
                        + "minuscolo viene ignorata senza alcun errore")
                .contains(PG_APPLICATION_NAME)
                .doesNotContain(PG_APPLICATION_NAME.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Il valore identifica l'<em>applicazione</em>, non il pod, e resta corto.
     *
     * <p>{@code application_name} è troncato da PostgreSQL a 63 byte, in silenzio e proprio sulla
     * coda. Includere l'hostname — che su Kubernetes è il nome del pod, già lungo di suo — vorrebbe
     * dire perdere l'unica parte che distingue un'istanza dall'altra. Quella domanda ha già una
     * risposta migliore in {@code pg_stat_activity.client_addr}.
     */
    @Test
    void ilNomeRestaEntroIlLimiteChePostgresNonTronca() throws IOException {
        String applicationName = boundDataSourceProperties().getProperty(PG_APPLICATION_NAME);

        assertThat(applicationName).hasSizeLessThanOrEqualTo(63);
        assertThat(applicationName)
                .as("deve nominare l'applicazione in modo riconoscibile accanto al backend")
                .contains("ingestor");
    }
}
