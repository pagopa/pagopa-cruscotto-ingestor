package it.pagopa.cruscotto.ingestion.configuration;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.entity.EntityName;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.quartz.CronTrigger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDetail;
import org.quartz.Trigger;

import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Blocca le due proprieta' Quartz da cui dipende la correttezza del deploy a piu' pod, e la
 * registrazione dei job FDR.
 *
 * <p><strong>Perche' valgono un test dedicato.</strong> Sono righe di configurazione il cui effetto
 * non si vede ne' in sviluppo (un pod solo) ne' in una suite: con un pod il comportamento e'
 * identico che siano impostate o no. Si manifestano in produzione, dove sono tre, e si manifestano
 * come <em>doppia esecuzione</em> — cioe' come dati duplicati, non come un errore. Il commento nel
 * codice le spiega, ma un commento non impedisce a nessuno di cambiare il valore.</p>
 *
 * <ul>
 *   <li>{@code isClustered=false} farebbe partire ogni trigger su <strong>tutti e tre</strong> i
 *       pod contemporaneamente: ogni finestra ingerita tre volte, e il
 *       {@code @DisallowConcurrentExecution} non protegge, perche' vale per JVM.</li>
 *   <li>{@code instanceId} non univoco farebbe registrare tutti i nodi sotto lo stesso id in
 *       {@code QRTZ_SCHEDULER_STATE}: il cluster manager non li distingue piu', il recupero al
 *       riavvio puo' rilanciare trigger ancora in corso su un altro pod, e il rilevamento dei nodi
 *       morti smette di funzionare. {@code AUTO} lascia derivare a Quartz un id univoco per pod.</li>
 * </ul>
 */
class QuartzConfigurationTest {

    private Properties props;

    @BeforeEach
    void setUp() {
        props = configurationWith(new FdrProperties()).quartzProperties();
    }

    @Test
    void loSchedulerEclusterizzato() {
        assertEquals("true", props.getProperty("org.quartz.jobStore.isClustered"),
                "senza clustering ogni trigger partirebbe su tutti i pod: stessa finestra ingerita N volte");
    }

    @Test
    void ogniNodoEsponeUnInstanceIdUnivoco() {
        assertEquals("AUTO", props.getProperty("org.quartz.scheduler.instanceId"),
                "con un instanceId fisso i pod non sono distinguibili in QRTZ_SCHEDULER_STATE: "
                        + "il recovery al riavvio puo' rilanciare trigger ancora in corso altrove");
    }

    /** Il prefisso deve portare lo schema: senza, Quartz cercherebbe le QRTZ_* nel search_path. */
    @Test
    void ilPrefissoDelleTabellePortaLoSchema() {
        assertEquals("cruscotto.QRTZ_", props.getProperty("org.quartz.jobStore.tablePrefix"));
    }

    /** Un pool a zero thread avvierebbe lo scheduler senza che nessun job possa mai girare. */
    @Test
    void ilPoolHaAlmenoUnThread() {
        int threadCount = Integer.parseInt(props.getProperty("org.quartz.threadPool.threadCount"));

        assertTrue(threadCount >= 1, "threadCount deve essere almeno 1, trovato " + threadCount);
    }

    // ─── FDR ─────────────────────────────────────────────────────────────────────

    /**
     * Il default e' il comportamento di oggi: un ambiente che non sa nulla di {@code fdr.*} non
     * deve ritrovarsi job nuovi.
     *
     * <p>Vale anche al livello sotto, ed e' la parte meno ovvia: i bean dei tre job sono
     * {@code @ConditionalOnProperty}, quindi a feature spenta non esistono. Un trigger registrato
     * lo stesso punterebbe a un job che Quartz non riesce a istanziare.
     */
    @Test
    void aFeatureSpentaNessunJobFdrEregistrato() {
        QuartzConfiguration configuration = configurationWith(new FdrProperties());

        assertThat(namesOf(configuration.buildJobDetails())).doesNotContain("fdrDailyJob",
                "fdrCommandPollerJob", "fdrRetryPollerJob");
        assertThat(triggerNamesOf(configuration)).doesNotContain("fdrDailyTrigger",
                "fdrCommandPollerTrigger", "fdrRetryPollerTrigger");
    }

    /**
     * Accendere FDR non accende da solo i tre scheduler: ciascuno ha il proprio interruttore.
     *
     * <p>Serve per poter attivare l'ingestione senza il poller dei comandi, o il contrario, senza
     * dover spegnere l'intera feature.
     */
    @Test
    void laFeatureAccesaNonAccendeDaSolaITreScheduler() {
        FdrProperties properties = new FdrProperties();
        properties.setEnabled(true);

        assertThat(triggerNamesOf(configurationWith(properties)))
                .doesNotContain("fdrDailyTrigger", "fdrCommandPollerTrigger",
                        "fdrRetryPollerTrigger");
    }

    @Test
    void ogniSchedulerFdrSiAccendeDaSolo() {
        assertThat(triggerNamesOf(configurationWith(fdrWith(true, false, false))))
                .contains("fdrDailyTrigger")
                .doesNotContain("fdrCommandPollerTrigger", "fdrRetryPollerTrigger");
        assertThat(triggerNamesOf(configurationWith(fdrWith(false, true, false))))
                .contains("fdrCommandPollerTrigger")
                .doesNotContain("fdrDailyTrigger", "fdrRetryPollerTrigger");
        assertThat(triggerNamesOf(configurationWith(fdrWith(false, false, true))))
                .contains("fdrRetryPollerTrigger")
                .doesNotContain("fdrDailyTrigger", "fdrCommandPollerTrigger");
    }

    @Test
    void ilCronDelDailyVieneDallaConfigurazione() {
        FdrProperties properties = fdrWith(true, false, false);
        properties.getDaily().setCron("0 */2 * * * ?");

        CronTrigger trigger = (CronTrigger) triggerNamed(configurationWith(properties),
                "fdrDailyTrigger");

        assertThat(trigger.getCronExpression()).isEqualTo("0 */2 * * * ?");
    }

    /**
     * Il fuso e' esplicito, e deve restarlo.
     *
     * <p>Senza, Quartz userebbe quello della JVM: "le 05:00" diventerebbe un orario che dipende
     * dall'immagine di base del container e che puo' cambiare a un aggiornamento, senza che nulla
     * lo segnali. Per un'elaborazione che deve stare dopo la pubblicazione dei file e prima
     * dell'orario di lavoro, un'ora di scarto conta.
     */
    @Test
    void iTriggerFdrPortanoUnFusoEsplicito() {
        FdrProperties properties = fdrWith(true, true, true);
        properties.getDaily().setTimezone("Europe/Rome");
        QuartzConfiguration configuration = configurationWith(properties);

        for (String name : List.of("fdrDailyTrigger", "fdrCommandPollerTrigger",
                "fdrRetryPollerTrigger")) {
            CronTrigger trigger = (CronTrigger) triggerNamed(configuration, name);
            assertThat(trigger.getTimeZone())
                    .as("fuso del trigger %s", name)
                    .isEqualTo(TimeZone.getTimeZone("Europe/Rome"));
        }
    }

    /** Il poller dei retry ha un cron suo, distinto dall'intervallo di un'ora fra due tentativi. */
    @Test
    void ilPollerDeiRetryHaUnCronDistintoDallIntervallo() {
        FdrProperties properties = fdrWith(false, false, true);

        CronTrigger trigger = (CronTrigger) triggerNamed(configurationWith(properties),
                "fdrRetryPollerTrigger");

        assertThat(trigger.getCronExpression()).isEqualTo(properties.getRetry().getPollCron());
        assertThat(properties.getRetry().getInterval()).isEqualTo(java.time.Duration.ofHours(1));
    }

    /**
     * Nomi distinti, su tutto lo scheduler.
     *
     * <p>Quartz identifica job e trigger per nome e gruppo: due omonimi non danno un errore, il
     * secondo <strong>sostituisce</strong> il primo — e con {@code setOverwriteExistingJobs(true)}
     * lo fa anche fra un deploy e l'altro. Il risultato sarebbe un job che smette di girare senza
     * che nulla lo dica.
     */
    @Test
    void nomiDiJobETriggerSonoTuttiDistinti() {
        QuartzConfiguration configuration = configurationWith(fdrWith(true, true, true));

        assertThat(namesOf(configuration.buildJobDetails())).doesNotHaveDuplicates();
        assertThat(triggerNamesOf(configuration)).doesNotHaveDuplicates();
    }

    /**
     * Ogni job FDR vieta la sovrapposizione con se stesso.
     *
     * <p>In DEV il cron del DAILY e' di un paio di minuti e una giornata ne dura di piu': senza,
     * due esecuzioni sulla stessa giornata si sovrapporrebbero regolarmente. Il cluster non basta —
     * impedisce che due <em>pod</em> ricevano lo stesso scatto, non che due scatti successivi si
     * accavallino sullo stesso pod.
     */
    @Test
    void iJobFdrNonSiSovrappongonoASeStessi() {
        for (Class<?> job : List.of(
                it.pagopa.cruscotto.ingestion.fdr.scheduler.QuartzFdrDailyJob.class,
                it.pagopa.cruscotto.ingestion.fdr.scheduler.QuartzFdrCommandPollerJob.class,
                it.pagopa.cruscotto.ingestion.fdr.scheduler.QuartzFdrRetryPollerJob.class)) {
            assertThat(job.isAnnotationPresent(DisallowConcurrentExecution.class))
                    .as("%s senza @DisallowConcurrentExecution", job.getSimpleName())
                    .isTrue();
        }
    }

    /**
     * I job FDR sono durevoli: esistono anche quando nessun trigger li punta.
     *
     * <p>Senza, Quartz cancellerebbe il JobDetail allo spegnimento del proprio interruttore, e
     * riaccenderlo richiederebbe di ricrearlo.
     */
    @Test
    void iJobFdrSonoDurevoli() {
        QuartzConfiguration configuration = configurationWith(fdrWith(false, false, false));
        FdrProperties enabled = new FdrProperties();
        enabled.setEnabled(true);

        for (JobDetail detail : configurationWith(enabled).buildJobDetails()) {
            assertThat(detail.isDurable()).as("%s non durevole", detail.getKey().getName()).isTrue();
        }
        assertThat(configuration.buildTriggers()).isNotNull();
    }

    // ─── Utilita' ────────────────────────────────────────────────────────────────

    /**
     * Una configurazione con i soli job ADX spenti.
     *
     * <p>Spegnerli non e' una semplificazione: toglie di mezzo la parte AS-IS e lascia nei trigger
     * solo quello che questo test deve osservare. Le loro voci vanno comunque dichiarate, perche'
     * l'assenza di una voce e' un errore di configurazione e non un "disabilitato" — ed e' una
     * distinzione che l'AS-IS fa di proposito.
     */
    private static QuartzConfiguration configurationWith(FdrProperties fdrProperties) {
        IngestionConfig ingestionConfig = new IngestionConfig();
        for (EntityName entity : EntityName.values()) {
            IngestionConfig.JobCronConfig job = new IngestionConfig.JobCronConfig();
            job.setEnabled(false);
            ingestionConfig.getQuartz().getJobs().put(entity, job);
        }
        ingestionConfig.getTokenRegistryCleanup().setEnabled(false);
        ingestionConfig.getStagingErrorCleanup().setEnabled(false);
        ingestionConfig.getBatchMetadataCleanup().setEnabled(false);
        ingestionConfig.getStatisticsRefresh().setEnabled(false);

        DbSchemaConfig dbSchemaConfig = new DbSchemaConfig();
        dbSchemaConfig.setSchema("cruscotto");

        return new QuartzConfiguration(ingestionConfig, dbSchemaConfig, new AppModeProperties(),
                fdrProperties);
    }

    private static FdrProperties fdrWith(boolean daily, boolean command, boolean retryPoll) {
        FdrProperties properties = new FdrProperties();
        properties.setEnabled(true);
        properties.getDaily().setEnabled(daily);
        properties.getCommand().setEnabled(command);
        properties.getRetry().setPollEnabled(retryPoll);
        return properties;
    }

    private static List<String> namesOf(List<JobDetail> details) {
        return details.stream().map(detail -> detail.getKey().getName()).toList();
    }

    private static List<String> triggerNamesOf(QuartzConfiguration configuration) {
        return configuration.buildTriggers().stream()
                .map(trigger -> trigger.getKey().getName())
                .toList();
    }

    private static Trigger triggerNamed(QuartzConfiguration configuration, String name) {
        return configuration.buildTriggers().stream()
                .filter(trigger -> trigger.getKey().getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("trigger non registrato: " + name));
    }
}
