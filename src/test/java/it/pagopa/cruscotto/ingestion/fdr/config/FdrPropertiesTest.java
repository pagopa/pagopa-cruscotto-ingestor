package it.pagopa.cruscotto.ingestion.fdr.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ancora la baseline di configurazione FDR.
 *
 * <p>Le due proprieta' che contano qui non sono i valori, sono le <strong>conseguenze</strong>:</p>
 * <ul>
 *   <li>un deployment che non conosce {@code fdr.*} deve continuare a partire. E' l'unica garanzia
 *       che rende questa aggiunta additiva invece che breaking, e si rompe in silenzio il giorno in
 *       cui qualcuno toglie la guardia sul flag dalla validazione;</li>
 *   <li>la classe deve restare un <strong>modello di configurazione</strong>. E' annotata
 *       {@code @Configuration}, quindi un {@code @Bean} aggiunto qui dentro verrebbe registrato
 *       davvero: basta un metodo per far comparire un job, un client o una connessione in uno
 *       stadio in cui nulla di tutto cio' deve esistere.</li>
 * </ul>
 */
class FdrPropertiesTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner().withUserConfiguration(TestConfig.class);

    @Configuration
    @EnableConfigurationProperties(FdrProperties.class)
    static class TestConfig {
    }

    // ─── Feature spenta ──────────────────────────────────────────────────────────

    /**
     * Il caso degli ambienti esistenti: nessuna chiave {@code fdr.*} fornita. Deve partire, e la
     * feature deve risultare spenta senza che nessuno l'abbia scritto da nessuna parte.
     */
    @Test
    void senzaAlcunaConfigurazioneIlContestoParteELaFeatureEspenta() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(FdrProperties.class).isEnabled()).isFalse();
        });
    }

    /**
     * A feature spenta la validazione non deve entrare nel merito: qui mancano container, batch size
     * e intervallo, e l'applicazione deve partire lo stesso. E' la regola che permette di rilasciare
     * questa baseline in produzione senza toccare la configurazione degli ambienti.
     */
    @Test
    void aFeatureSpentaLeProprietaOperativeMancantiNonBloccanoLAvvio() {
        runner.withPropertyValues(
                        "fdr.enabled=false",
                        "fdr.processing.batch-size=0",
                        "fdr.retry.max-attempts=0")
                .run(context -> assertThat(context).hasNotFailed());
    }

    // ─── Default ─────────────────────────────────────────────────────────────────

    /**
     * I default dichiarati. {@code enabled=false} e' il piu' importante: e' l'opposto di
     * {@code massive-search.enabled}, e invertirlo accenderebbe la feature ovunque al primo deploy.
     * I due valori del retry sono quelli confermati dal cliente — 5 tentativi a un'ora di distanza.
     * Il container ha un default da quando il nome e' un fatto confermato, come per
     * {@code massive-search.storage.container}.
     */
    @Test
    void iDefaultSonoQuelliDichiarati() {
        runner.run(context -> {
            FdrProperties properties = context.getBean(FdrProperties.class);

            assertThat(properties.isEnabled()).isFalse();
            assertThat(properties.getStorage().getContainer()).isEqualTo("fdr");
            assertThat(properties.getProcessing().getBatchSize()).isEqualTo(1000);
            assertThat(properties.getRetry().getMaxAttempts()).isEqualTo(5);
            assertThat(properties.getRetry().getInterval()).isEqualTo(Duration.ofHours(1));
        });
    }

    /**
     * Non esiste un {@code base-path}. Il layout e' confermato — le cartelle giornaliere stanno
     * alla radice del container — quindi non c'e' alcun prefisso statico da configurare, e la
     * giornata e' dinamica. La property era nata come knob in attesa della convenzione; quella
     * ragione e' scaduta.
     */
    @Test
    void nonEsisteUnBasePathStatico() {
        assertThat(Arrays.stream(FdrProperties.Storage.class.getDeclaredFields())
                .filter(field -> !field.isSynthetic())
                .map(Field::getName)
                .toList())
                .as("la configurazione dello storage FDR deve contenere solo il container")
                .containsExactly("container");
    }

    // ─── Binding ─────────────────────────────────────────────────────────────────

    @Test
    void aFeatureAccesaLeProprietaVengonoLegateTutte() {
        runner.withPropertyValues(
                        "fdr.enabled=true",
                        "fdr.storage.container=fdr-collaudo",
                        "fdr.processing.batch-size=250",
                        "fdr.retry.max-attempts=3",
                        "fdr.retry.interval=30m")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    FdrProperties properties = context.getBean(FdrProperties.class);

                    assertThat(properties.isEnabled()).isTrue();
                    assertThat(properties.getStorage().getContainer()).isEqualTo("fdr-collaudo");
                    assertThat(properties.getProcessing().getBatchSize()).isEqualTo(250);
                    assertThat(properties.getRetry().getMaxAttempts()).isEqualTo(3);
                    assertThat(properties.getRetry().getInterval()).isEqualTo(Duration.ofMinutes(30));
                });
    }

    /**
     * Il container resta sovrascrivibile per ambiente: il default non deve averlo inchiodato. Senza
     * questa garanzia il nome finirebbe di fatto nel codice, che e' il motivo per cui non va
     * scritto dentro il client.
     *
     * <p>Verifica il <strong>binding</strong> del valore custom, non l'avvio: e' il test gemello di
     * {@link #unaConfigurazioneCompletaSuperaLaValidazione()}, che verifica invece che una
     * configurazione completa non venga rifiutata. Due garanzie diverse sullo stesso scenario: una
     * puo' rompersi senza l'altra — un binding sbagliato lascia partire il contesto con il
     * container di default, e una validazione troppo severa lo blocca pur avendolo legato bene.</p>
     */
    @Test
    void ilContainerRestaSovrascrivibilePerAmbiente() {
        runner.withPropertyValues("fdr.enabled=true", "fdr.storage.container=fdr-uat")
                .run(context -> assertThat(context.getBean(FdrProperties.class)
                        .getStorage().getContainer()).isEqualTo("fdr-uat"));
    }

    /**
     * Una configurazione FDR completa e valida, con la feature accesa, deve superare la validazione
     * e lasciar partire il contesto.
     *
     * <p>E' la garanzia complementare a quella sopra, e guarda dal lato opposto: li' si verifica
     * che il valore arrivi, qui che l'insieme non venga <em>rifiutato</em>. Un irrigidimento della
     * validazione — un controllo nuovo scritto senza pensare al caso buono — romperebbe questo e
     * non quello.</p>
     */
    @Test
    void unaConfigurazioneCompletaSuperaLaValidazione() {
        runner.withPropertyValues(
                        "fdr.enabled=true",
                        "fdr.storage.container=fdr-collaudo",
                        "fdr.processing.batch-size=500",
                        "fdr.retry.max-attempts=3",
                        "fdr.retry.interval=15m")
                .run(context -> assertThat(context).hasNotFailed());
    }

    /**
     * {@code application.yml} valorizza container e base path con un segnaposto che, senza variabile
     * d'ambiente, si risolve nella <strong>stringa vuota</strong> e non in {@code null}. Il controllo
     * deve trattarla come assente, altrimenti a feature accesa un container non configurato passerebbe
     * la validazione e fallirebbe molto piu' tardi, alla prima lettura.
     */
    @Test
    void ilContainerVuotoEtrattatoComeAssente() {
        runner.withPropertyValues("fdr.enabled=true", "fdr.storage.container=")
                .run(context -> assertThat(context).hasFailed());
    }

    /**
     * Il binding sul file VERO, non su property sintetiche.
     *
     * <p>Tutti gli altri test di questa classe usano {@code withPropertyValues}, quindi verificano
     * la classe ma non il YAML: un refuso nella sezione {@code fdr:} di {@code application.yml} —
     * {@code container-name} invece di {@code container}, un livello di indentazione sbagliato —
     * non farebbe fallire nulla. La property resterebbe semplicemente al suo default e l'errore
     * comparirebbe in esercizio, come una configurazione che "non viene letta".</p>
     *
     * <p>Le property source di sistema vengono rimosse di proposito: senza, un
     * {@code FDR_ENABLED} impostato sulla macchina di chi esegue i test cambierebbe il risultato,
     * e un test che dipende dall'ambiente e' peggio di nessun test.</p>
     */
    @Test
    void laSezioneFdrDiApplicationYmlSiLegaDavvero() throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources()
                .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources()
                .remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"))
                .forEach(source -> environment.getPropertySources().addFirst(source));

        // PRIMA le chiavi, POI i valori, e l'ordine non e' estetico. Se una chiave venisse
        // rinominata, il binder ripiegherebbe in silenzio sul default dichiarato nel campo - che
        // per il container vale "fdr", cioe' ESATTAMENTE il valore atteso. Il solo confronto sui
        // valori sarebbe quindi cieco proprio sul difetto che questo test esiste per trovare.
        List.of("fdr.enabled",
                        "fdr.storage.container",
                        "fdr.processing.batch-size",
                        "fdr.retry.max-attempts",
                        "fdr.retry.interval")
                .forEach(key -> assertThat(environment.containsProperty(key))
                        .as("chiave assente o rinominata in application.yml: %s", key)
                        .isTrue());

        FdrProperties bound = Binder.get(environment)
                .bind("fdr", FdrProperties.class)
                .orElseThrow(() -> new AssertionError(
                        "la sezione fdr: di application.yml non si lega a FdrProperties"));

        assertThat(bound.isEnabled())
                .as("FDR deve restare spento negli ambienti esistenti")
                .isFalse();
        assertThat(bound.getStorage().getContainer()).isEqualTo("fdr");
        assertThat(bound.getProcessing().getBatchSize()).isEqualTo(1000);
        assertThat(bound.getRetry().getMaxAttempts()).isEqualTo(5);
        assertThat(bound.getRetry().getInterval()).isEqualTo(Duration.ofHours(1));
    }

    /**
     * Nessuna property {@code fdr.*} deve esistere in {@code application.yml} senza una controparte
     * nel modello: una chiave di troppo e' configurazione che qualcuno crede attiva e che invece
     * non viene letta da nessuno. E' il difetto gemello del refuso, e si vede solo da questo lato.
     */
    @Test
    void applicationYmlNonDichiaraPropertyFdrSconosciute() throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources()
                .remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources()
                .remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"))
                .forEach(source -> environment.getPropertySources().addFirst(source));

        Set<String> declared = Set.of("fdr.enabled",
                "fdr.storage.container",
                "fdr.processing.batch-size",
                "fdr.retry.max-attempts",
                "fdr.retry.interval",
                "fdr.retry.poll-enabled",
                "fdr.retry.poll-cron",
                "fdr.daily.enabled",
                "fdr.daily.cron",
                "fdr.daily.timezone",
                "fdr.daily.override-business-day",
                "fdr.command.enabled",
                "fdr.command.cron",
                "fdr.recovery.stale-after");

        List<String> unknown = environment.getPropertySources().stream()
                .filter(EnumerablePropertySource.class::isInstance)
                .map(EnumerablePropertySource.class::cast)
                .flatMap(source -> Arrays.stream(source.getPropertyNames()))
                .filter(name -> name.startsWith("fdr."))
                .distinct()
                .filter(name -> !declared.contains(name))
                .toList();

        assertThat(unknown)
                .as("property fdr.* in application.yml che nessuno legge: %s", unknown)
                .isEmpty();
    }

    // ─── Validazione a feature accesa ────────────────────────────────────────────

    /**
     * Il container ha un default, quindi accendere la feature senza configurare lo storage basta.
     * Prima della conferma del nome questo stesso scenario doveva <em>fallire</em>: la validazione
     * non e' sparita, e' il valore mancante che ora non e' piu' un errore. Lo scenario che deve
     * ancora fallire — il container esplicitamente svuotato — e' coperto sopra.
     */
    @Test
    void aFeatureAccesaIlDefaultDelContainerBastaPerPartire() {
        runner.withPropertyValues("fdr.enabled=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(FdrProperties.class).getStorage().getContainer())
                            .isEqualTo("fdr");
                });
    }

    /**
     * Uno spazio nel nome del container e' il difetto di configurazione peggiore della famiglia:
     * invisibile in ogni listato e in ogni log. Supera il controllo di stringa vuota, arriva ad
     * Azure e torna indietro come "container inesistente" alla prima lettura, lontanissimo dalla
     * causa. Qui deve fermare l'avvio, e il messaggio deve mostrare il valore fra virgolette
     * perche' il carattere si veda.
     *
     * <p>La property viene iniettata direttamente nell'{@code Environment} e non con
     * {@code withPropertyValues}, che <strong>fa trim</strong> dei valori inline: passando di li'
     * lo scenario non sarebbe riproducibile. Non e' un trucco per far passare il test, e'
     * fedelta' al percorso reale — anche il parser YAML scarta gli spazi finali di uno scalare,
     * quindi il valore sporco che sopravvive davvero e' quello che arriva da una variabile
     * d'ambiente o da un ConfigMap, dove nessuno lo normalizza.</p>
     */
    @Test
    void unContainerConSpaziNonPassaLaValidazione() {
        new ApplicationContextRunner()
                .withUserConfiguration(TestConfig.class)
                .withInitializer(context -> context.getEnvironment().getPropertySources()
                        .addFirst(new MapPropertySource("ambiente-sporco", Map.of(
                                "fdr.enabled", "true",
                                "fdr.storage.container", "fdr "))))
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context).getFailure()
                            .hasStackTraceContaining("must not contain whitespace")
                            .hasStackTraceContaining("\"fdr \"");
                });
    }

    @Test
    void aFeatureAccesaIlBatchSizeDeveEssereMaggioreDiZero() {
        runner.withPropertyValues(
                        "fdr.enabled=true",
                        "fdr.storage.container=fdr-drops",
                        "fdr.processing.batch-size=0")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context).getFailure()
                            .hasStackTraceContaining("fdr.processing.batch-size");
                });
    }

    @Test
    void aFeatureAccesaITentativiDevonoEssereMaggioriDiZero() {
        runner.withPropertyValues(
                        "fdr.enabled=true",
                        "fdr.storage.container=fdr-drops",
                        "fdr.retry.max-attempts=0")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context).getFailure()
                            .hasStackTraceContaining("fdr.retry.max-attempts");
                });
    }

    @Test
    void aFeatureAccesaLIntervalloDeveEsserePositivo() {
        runner.withPropertyValues(
                        "fdr.enabled=true",
                        "fdr.storage.container=fdr-drops",
                        "fdr.retry.interval=0s")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context).getFailure()
                            .hasStackTraceContaining("fdr.retry.interval");
                });
    }


    // ─── Isolamento ──────────────────────────────────────────────────────────────

    /**
     * La baseline non deve portare in vita nient'altro che se stessa: nessun job, nessun trigger,
     * nessun client, nessuna connessione. {@code FdrProperties} e' {@code @Configuration}, quindi un
     * {@code @Bean} scritto al suo interno verrebbe registrato per davvero — questo test e' cio' che
     * se ne accorge.
     */
    @Test
    void laBaselineNonRegistraNessunAltroBean() {
        runner.withPropertyValues("fdr.enabled=true", "fdr.storage.container=fdr-drops")
                .run(context -> {
                    List<String> applicationBeans = Arrays.stream(context.getBeanDefinitionNames())
                            .filter(name -> !name.startsWith("org.springframework"))
                            .filter(name -> !(context.getBean(name) instanceof TestConfig))
                            .toList();

                    assertThat(applicationBeans)
                            .as("la baseline deve registrare solo il modello di configurazione: %s",
                                    applicationBeans)
                            .hasSize(1);
                    assertThat(context.getBean(applicationBeans.get(0)))
                            .isInstanceOf(FdrProperties.class);
                });
    }

    /**
     * Nessun metodo {@code @Bean} dichiarato. E' la stessa garanzia del test sopra vista dal lato
     * della classe, e regge anche se un domani il contesto di test cambiasse forma.
     */
    @Test
    void ilModelloDiConfigurazioneNonDichiaraMetodiBean() {
        List<String> beanMethods = Arrays.stream(FdrProperties.class.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(
                        org.springframework.context.annotation.Bean.class))
                .map(Method::getName)
                .toList();

        assertThat(beanMethods).isEmpty();
    }

    /**
     * Il segreto dello storage account NON deve poter atterrare qui.
     *
     * <p>La connection string vive una volta sola, sotto {@code azure.blob}, ed e' condivisa con la
     * ricerca massiva: duplicarla sotto {@code fdr.*} significherebbe due segreti da ruotare, due
     * punti da sbagliare e un valore riservato in piu' nei ConfigMap. E' un errore che si commette
     * con le migliori intenzioni, scrivendo "tanto serve anche a FDR".</p>
     *
     * <p>Il controllo e' generico sulle classi annidate dichiarate, quindi copre anche una nuova
     * sezione aggiunta domani.</p>
     */
    @Test
    void laConfigurazioneFdrNonPuoOspitareUnSegreto() {
        List<String> suspicious = Stream.concat(
                        Stream.of(FdrProperties.class),
                        Arrays.stream(FdrProperties.class.getDeclaredClasses()))
                .flatMap(type -> Arrays.stream(type.getDeclaredFields()))
                .filter(field -> !field.isSynthetic())
                .map(Field::getName)
                .filter(name -> name.toLowerCase(Locale.ROOT)
                        .matches(".*(secret|password|credential|connectionstring|accountkey|sastoken).*"))
                .toList();

        assertThat(suspicious)
                .as("il segreto dello storage sta sotto azure.blob e non va duplicato: %s", suspicious)
                .isEmpty();
    }

    /**
     * Nessun collaboratore fra i campi: solo valori e le classi annidate. Un client Azure, un
     * repository o uno scheduler iniettati qui dentro aprirebbero una connessione all'avvio, cioe'
     * esattamente cio' che questa baseline dichiara di non fare.
     */
    @Test
    void ilModelloDiConfigurazioneNonHaCollaboratori() {
        Set<Class<?>> allowed = Set.of(
                boolean.class, int.class, long.class, String.class, Duration.class,
                FdrProperties.Storage.class, FdrProperties.Processing.class,
                FdrProperties.Retry.class, FdrProperties.Daily.class, FdrProperties.Command.class,
                FdrProperties.Recovery.class);

        List<Class<?>> fieldTypes = Arrays.stream(FdrProperties.class.getDeclaredFields())
                .filter(field -> !field.isSynthetic())
                // il logger di @Slf4j e' statico: e' parte dell'idioma, non un collaboratore
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .map(Field::getType)
                .filter(type -> !allowed.contains(type))
                .toList();

        assertThat(fieldTypes)
                .as("campo non previsto nel modello di configurazione: %s", fieldTypes)
                .isEmpty();
    }

    // ─── Scheduler ───────────────────────────────────────────────────────────────

    /**
     * I tre scheduler nascono spenti, e quelli degli ambienti esistenti devono restare tali.
     *
     * <p>Accendere {@code fdr.enabled} per provare la discovery non deve far partire da solo un
     * DAILY che scrive su database.
     */
    @Test
    void iTreSchedulerNasconoSpenti() {
        runner.run(context -> {
            FdrProperties properties = context.getBean(FdrProperties.class);
            assertThat(properties.getDaily().isEnabled()).isFalse();
            assertThat(properties.getCommand().isEnabled()).isFalse();
            assertThat(properties.getRetry().isPollEnabled()).isFalse();
        });
    }

    /** Il default di produzione: le 05:00, nel fuso italiano, e nessun override. */
    @Test
    void ilDefaultDelDailyEleCinqueDelMattinoOraItaliana() {
        runner.run(context -> {
            FdrProperties.Daily daily = context.getBean(FdrProperties.class).getDaily();
            assertThat(daily.getCron()).isEqualTo("0 0 5 * * ?");
            assertThat(daily.getTimezone()).isEqualTo("Europe/Rome");
            assertThat(daily.getOverrideBusinessDay())
                    .as("un override lasciato in produzione rielaborerebbe per sempre la stessa"
                            + " cartella, senza mai ingerire le giornate correnti")
                    .isNullOrEmpty();
        });
    }

    /**
     * La modalita' DEV e' solo configurazione: stesso codice, altri valori.
     *
     * <p>Un ramo {@code if (dev)} nel Java farebbe percorrere a DEV righe che la produzione non
     * esegue mai, cioe' renderebbe la verifica in DEV una verifica di qualcos'altro.
     */
    @Test
    void laModalitaDevEsoloConfigurazione() {
        runner.withPropertyValues("fdr.enabled=true",
                        "fdr.daily.enabled=true",
                        "fdr.daily.cron=0 */2 * * * ?",
                        "fdr.daily.override-business-day=2026-09-15",
                        "fdr.command.enabled=true",
                        "fdr.command.cron=0/30 * * * * ?",
                        "fdr.retry.poll-enabled=true",
                        "fdr.retry.poll-cron=0/30 * * * * ?",
                        "fdr.retry.interval=1m")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    FdrProperties properties = context.getBean(FdrProperties.class);
                    assertThat(properties.getDaily().getCron()).isEqualTo("0 */2 * * * ?");
                    assertThat(properties.getDaily().getOverrideBusinessDay())
                            .isEqualTo("2026-09-15");
                    assertThat(properties.getCommand().getCron()).isEqualTo("0/30 * * * * ?");
                    assertThat(properties.getRetry().getPollCron()).isEqualTo("0/30 * * * * ?");
                    assertThat(properties.getRetry().getInterval())
                            .isEqualTo(Duration.ofMinutes(1));
                });
    }

    /**
     * Un cron malformato deve fermare l'avvio.
     *
     * <p>Senza il controllo, Quartz fallirebbe la costruzione del trigger in una riga di log allo
     * startup e il pod resterebbe su, apparentemente sano, con uno scheduler in meno.
     */
    @Test
    void unCronMalformatoImpediscelAvvio() {
        runner.withPropertyValues("fdr.enabled=true", "fdr.daily.enabled=true",
                        "fdr.daily.cron=ogni mattina")
                .run(context -> assertThat(context).hasFailed());
    }

    /**
     * Un fuso sconosciuto deve fermare l'avvio, ed e' il controllo meno ovvio dei tre.
     *
     * <p>{@code TimeZone.getTimeZone} non fallisce mai: a un identificativo che non riconosce
     * risponde GMT. Il DAILY si sposterebbe quindi di un'ora o due e nulla lo segnalerebbe.
     */
    @Test
    void unFusoSconosciutoImpediscelAvvio() {
        runner.withPropertyValues("fdr.enabled=true", "fdr.daily.enabled=true",
                        "fdr.daily.timezone=Europe/Roma")
                .run(context -> assertThat(context).hasFailed());
    }

    /** Un override che non e' una data si scoprirebbe solo al primo scatto del cron. */
    @Test
    void unOverrideNonInterpretabileImpediscelAvvio() {
        runner.withPropertyValues("fdr.enabled=true", "fdr.daily.override-business-day=ieri")
                .run(context -> assertThat(context).hasFailed());
    }

    /** I controlli sugli scheduler valgono solo a feature accesa, come tutti gli altri. */
    @Test
    void aFeatureSpentaNessunControlloSugliScheduler() {
        runner.withPropertyValues("fdr.daily.cron=non un cron",
                        "fdr.daily.timezone=Europe/Roma",
                        "fdr.daily.override-business-day=ieri")
                .run(context -> assertThat(context).hasNotFailed());
    }
}
