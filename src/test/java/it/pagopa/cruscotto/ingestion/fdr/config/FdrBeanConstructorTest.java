package it.pagopa.cruscotto.ingestion.fdr.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifica che ogni bean FDR sia <strong>istanziabile da Spring</strong>.
 *
 * <h2>Il difetto che questo test esiste per impedire</h2>
 *
 * <p>Sei servizi FDR hanno due costruttori: quello di produzione e uno aggiuntivo che prende un
 * {@code Clock} o un client già costruito, usato dai test per rendere deterministico il tempo o per
 * evitare Azure. È un pattern sano, ma ha un effetto collaterale che non si vede leggendo la classe:
 * <strong>quando i costruttori sono più di uno e nessuno è annotato, Spring non ne sceglie
 * nessuno</strong>. Ricade sul costruttore senza argomenti, che non esiste, e il contesto muore con
 * {@code BeanInstantiationException: No default constructor found}.
 *
 * <p>Conseguenza reale: con {@code fdr.enabled=true} l'applicazione <em>non partiva</em>, in nessun
 * ambiente. Il difetto è emerso solo al primo avvio con la feature accesa.
 *
 * <h2>Perché nessun test lo aveva visto</h2>
 *
 * <p>Tutti i test dei servizi FDR costruiscono l'oggetto con {@code new}, scegliendo il costruttore
 * a mano — cioè facendo esattamente ciò che Spring si rifiuta di fare. La risoluzione del
 * costruttore non veniva esercitata da nessuna parte, e il verde della suite non diceva nulla
 * sull'avvio.
 *
 * <h2>Perché la forma è questa e non un contesto Spring vero</h2>
 *
 * <p>Un {@code ApplicationContextRunner} proverebbe l'avvio per davvero, ma richiede di ricostruire
 * a mano il grafo delle dipendenze con una decina di mock — e i due bean di storage costruiscono il
 * client Azure <em>dentro</em> il costruttore, quindi andrebbero sostituiti da finti, cioè proprio i
 * bean di cui non si verificherebbe più nulla. Soprattutto coprirebbe le sei classi di oggi e non la
 * settima, che è il caso che conta: il prossimo che aggiunge un costruttore per i test.
 *
 * <p>Questo test applica invece la stessa regola che applica Spring, a <strong>tutti</strong> gli
 * stereotipi del package FDR, presenti e futuri. Chi lo vedesse fallire non ha rotto il test: ha
 * scritto un bean che non si avvia.
 */
class FdrBeanConstructorTest {

    private static final String FDR_PACKAGE = "it.pagopa.cruscotto.ingestion.fdr";

    @Test
    @DisplayName("ogni bean FDR con più costruttori ne dichiara uno a Spring")
    void ogniBeanFdrEIstanziabileDaSpring() {
        List<String> nonIstanziabili = new ArrayList<>();

        for (Class<?> bean : beanFdr()) {
            Constructor<?>[] costruttori = bean.getDeclaredConstructors();
            if (costruttori.length <= 1) {
                // Con un solo costruttore Spring lo usa senza bisogno di annotazioni.
                continue;
            }
            long annotati = 0;
            boolean senzaArgomenti = false;
            for (Constructor<?> c : costruttori) {
                if (c.isAnnotationPresent(Autowired.class)) {
                    annotati++;
                }
                if (c.getParameterCount() == 0) {
                    senzaArgomenti = true;
                }
            }
            if (annotati == 0 && !senzaArgomenti) {
                nonIstanziabili.add(bean.getSimpleName() + ": " + costruttori.length
                        + " costruttori, nessuno annotato @Autowired e nessuno senza argomenti");
            }
            if (annotati > 1) {
                // Spring rifiuta piu' di un costruttore annotato come required.
                nonIstanziabili.add(bean.getSimpleName() + ": " + annotati
                        + " costruttori annotati @Autowired, ne e' ammesso uno solo");
            }
        }

        assertThat(nonIstanziabili)
                .as("bean FDR che Spring non riuscirebbe a istanziare all'avvio")
                .isEmpty();
    }

    /**
     * Il test ha senso solo se qualcosa trova davvero.
     *
     * <p>Senza questa verifica, una rinomina del package renderebbe la scansione vuota e il controllo
     * sopra passerebbe sempre — fallendo in silenzio nel modo peggiore, cioè sembrando sano.
     */
    @Test
    @DisplayName("la scansione del package FDR trova i bean")
    void laScansioneTrovaIBeanFdr() {
        assertThat(beanFdr())
                .as("stereotipi Spring nel package " + FDR_PACKAGE)
                .hasSizeGreaterThan(10);
    }

    /**
     * Gli stereotipi del package FDR.
     *
     * <p>Il filtro è su {@link Component}, che è meta-annotazione di {@code @Service} e
     * {@code @Repository}: li prende tutti e tre con una regola sola. Le classi astratte sono
     * escluse dal provider, quindi restano solo i candidati che Spring proverebbe a istanziare.
     *
     * <p><strong>L'ambiente con {@code fdr.enabled=true} non è un dettaglio.</strong> Il provider
     * valuta le {@code @Conditional} durante la scansione, e ogni bean FDR è condizionato a quella
     * property: senza, la scansione ne trova <em>due</em> su venti — i soli non condizionati — e il
     * controllo sui costruttori girerebbe praticamente a vuoto. È il motivo per cui
     * {@link #laScansioneTrovaIBeanFdr()} esiste: questo errore è già successo scrivendo il test, e
     * senza quell'asserzione sarebbe passato per verde.
     */
    private static List<Class<?>> beanFdr() {
        MockEnvironment environment = new MockEnvironment();
        environment.setProperty("fdr.enabled", "true");

        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false, environment);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Component.class));

        List<Class<?>> beans = new ArrayList<>();
        scanner.findCandidateComponents(FDR_PACKAGE).forEach(definition -> {
            try {
                beans.add(Class.forName(definition.getBeanClassName()));
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(
                        "classe trovata dalla scansione ma non caricabile: "
                                + definition.getBeanClassName(), e);
            }
        });
        return beans;
    }
}
