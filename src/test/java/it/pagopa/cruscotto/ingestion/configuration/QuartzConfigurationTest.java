package it.pagopa.cruscotto.ingestion.configuration;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.ingestor.IngestionConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Blocca le due proprieta' Quartz da cui dipende la correttezza del deploy a piu' pod.
 *
 * <p><strong>Perche' valgono un test dedicato.</strong> Sono due righe di configurazione il cui
 * effetto non si vede ne' in sviluppo (un pod solo) ne' in una suite: con un pod il comportamento e'
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
        IngestionConfig ingestionConfig = new IngestionConfig();
        DbSchemaConfig dbSchemaConfig = new DbSchemaConfig();
        dbSchemaConfig.setSchema("cruscotto");

        QuartzConfiguration configuration =
                new QuartzConfiguration(ingestionConfig, dbSchemaConfig, new AppModeProperties());

        props = configuration.quartzProperties();
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
}
