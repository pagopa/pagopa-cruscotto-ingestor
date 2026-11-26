package it.pagopa.cruscotto.ingestion.fdr;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Il modulo FDR non dipende dalle strutture operative del vecchio ingestor.
 *
 * <h2>Perché un test strutturale e non uno di comportamento</h2>
 *
 * <p>La separazione è una proprietà del <em>grafo delle dipendenze</em>, non di un'esecuzione: un
 * test di comportamento la verificherebbe sul percorso che esercita e tacerebbe su tutti gli altri.
 * Qui la domanda è invece assoluta — "da nessuna parte, in nessun percorso" — e l'unico modo di
 * rispondere è guardare tutto il codice.
 *
 * <p>Vale soprattutto per il <strong>futuro</strong>. Le classi che accoppiavano FDR all'AS-IS sono
 * state rimosse, quindi oggi il controllo passerebbe anche se non esistesse; serve al prossimo che,
 * dovendo registrare una riga scartata, troverà {@code StagingErrorService} già pronto e
 * iniettabile e non avrà alcun modo di sapere che FDR se n'è separato di proposito. Il messaggio di
 * fallimento glielo dice.
 *
 * <h2>Perché si leggono i sorgenti e non il bytecode</h2>
 *
 * <p>Un'analisi del bytecode vedrebbe gli import ma <strong>non i nomi di tabella</strong>, che in
 * questo progetto sono stringhe SQL costruite a mano. Metà dell'accoppiamento da impedire è
 * esattamente quello: una query su {@code INGEST_EXECUTION_LOG} non lascia traccia nel grafo dei
 * tipi. I commenti vengono tolti prima del confronto, perché spiegare perché una cosa non c'è più è
 * legittimo — ed è anzi ciò che fa il resto del modulo.
 */
class FdrOperationalDecouplingTest {

    private static final Path FDR_SOURCES =
            Path.of("src/main/java/it/pagopa/cruscotto/ingestion/fdr");

    /**
     * Ciò che FDR non deve più nominare, con la ragione che finisce nel messaggio di fallimento.
     *
     * <p>Ogni voce è una decisione presa, non un divieto di stile: chi vedrà fallire questo test
     * deve poter capire <em>perché</em> senza andare a cercare l'handover.
     */
    private static final Map<String, String> FORBIDDEN = Map.ofEntries(
            Map.entry("STG_INGEST_ERROR",
                    "lo staging dell'ingestione ADX ha una sua retention e un suo percorso di "
                            + "riconciliazione: una riga FDR messa li' non verrebbe riconciliata da "
                            + "nessuno. Gli scarti FDR sono conteggi su FDR_INGESTION_FILE piu' un "
                            + "evento aggregato ROWS_DISCARDED"),
            Map.entry("StagingErrorService", "vedi STG_INGEST_ERROR"),
            Map.entry("insertDiscardedBulk", "vedi STG_INGEST_ERROR"),
            Map.entry("insertBulk",
                    "scrive righe PENDING destinate alla riconciliazione ADX, che FDR non "
                            + "attraversa: resterebbero PENDING per sempre"),
            Map.entry("INGEST_EXECUTION_LOG",
                    "lo stato di una giornata FDR lo dice FDR_INGESTION_STATUS e la liveness la "
                            + "dice il lease. Tenerlo anche li' significherebbe due verita' "
                            + "destinate a divergere"),
            Map.entry("ExecutionLogService", "vedi INGEST_EXECUTION_LOG"),
            Map.entry("ExecutionLogReaper",
                    "il reaper dell'AS-IS chiude le esecuzioni ADX: le giornate FDR abbandonate si "
                            + "riconoscono dal lease scaduto"),
            Map.entry("TrackedJobExecutor",
                    "runTracked apre e chiude obbligatoriamente una riga in INGEST_EXECUTION_LOG. "
                            + "I job FDR usano FdrQuartzJobExecutor"),
            Map.entry("job_schedules",
                    "e' il cursore dell'ingestione ADX. FDR non ha checkpoint: la giornata gliela "
                            + "dice lo scheduler, o la richiesta a database"),
            Map.entry("BulkWriterImpl",
                    "lo scrittore dell'ingestione ADX. FDR ha FdrRowWriter"),
            Map.entry("WindowCyclePersistenceService",
                    "il ciclo a finestre dell'ingestione ADX. FDR elabora giornate, non finestre"),
            Map.entry("JobLauncher",
                    "FDR non esegue Job o Step Spring Batch e non scrive nei metadata BATCH_*"),
            Map.entry("JobRepository", "vedi JobLauncher"));

    @Test
    @DisplayName("il runtime FDR non nomina nessuna struttura operativa del vecchio ingestor")
    void ilRuntimeFdrNonDipendeDalleStruttureOperativeAsIs() {
        List<String> violations = new ArrayList<>();

        for (Path source : fdrSources()) {
            String code = withoutComments(read(source));
            FORBIDDEN.forEach((token, reason) -> {
                if (code.contains(token)) {
                    violations.add(FDR_SOURCES.relativize(source) + " nomina " + token
                            + " — " + reason);
                }
            });
        }

        assertThat(violations)
                .as("riferimenti del runtime FDR a strutture operative dell'ingestione ADX")
                .isEmpty();
    }

    /**
     * Il controllo ha senso solo se legge davvero qualcosa.
     *
     * <p>Una rinomina del package renderebbe la scansione vuota e il test sopra passerebbe sempre —
     * fallendo nel modo peggiore, cioè sembrando sano. È la stessa rete che
     * {@code FdrBeanConstructorTest} si è dovuto dare dopo che una scansione a vuoto era passata
     * per verde.
     */
    @Test
    @DisplayName("la scansione dei sorgenti FDR trova i file")
    void laScansioneTrovaISorgentiFdr() {
        assertThat(fdrSources())
                .as("sorgenti sotto " + FDR_SOURCES)
                .hasSizeGreaterThan(30);
    }

    /**
     * Il percorso che scrive dato business passa <strong>sempre</strong> da una chiusura.
     *
     * <p>{@code FdrDayFence.NONE} esiste per il livello di scrittura provato in isolamento e per il
     * preflight, che legge senza scrivere; usarla dall'orchestrazione riaprirebbe esattamente la
     * finestra che la chiusura esiste per chiudere — due worker che scrivono la stessa giornata e
     * la raddoppiano in silenzio, perché l'INSERT è fedele e non deduplica.
     */
    @Test
    @DisplayName("l'orchestrazione non usa mai la chiusura permissiva")
    void lOrchestrazioneNonUsaMaiLaChiusuraPermissiva() {
        List<String> violations = new ArrayList<>();
        for (Path source : fdrSources()) {
            if (!source.toString().contains("workflow")) {
                continue;
            }
            if (withoutComments(read(source)).contains("FdrDayFence.NONE")) {
                violations.add(FDR_SOURCES.relativize(source).toString());
            }
        }
        assertThat(violations)
                .as("classi di orchestrazione che scrivono senza dimostrare il possesso")
                .isEmpty();
    }

    private static List<Path> fdrSources() {
        try (Stream<Path> files = Files.walk(FDR_SOURCES)) {
            return files.filter(path -> path.toString().endsWith(".java")).toList();
        } catch (IOException e) {
            throw new UncheckedIOException("sorgenti FDR non leggibili da " + FDR_SOURCES, e);
        }
    }

    private static String read(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("sorgente non leggibile: " + path, e);
        }
    }

    /**
     * Toglie commenti di blocco e di riga.
     *
     * <p>Deliberatamente grossolano: non capisce le stringhe che contengono {@code //}, e va bene
     * così. L'effetto è al più un falso negativo su una stringa che contenga un commento finto,
     * mentre la direzione pericolosa — un falso positivo che blocchi la build su una spiegazione —
     * resta esclusa. Nel modulo i nomi vietati compaiono quasi solo nei commenti che raccontano
     * perché non ci sono più: senza questa pulizia il test fallirebbe sulla propria documentazione.
     */
    private static String withoutComments(String source) {
        return source
                .replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("(?m)//.*$", "");
    }
}
