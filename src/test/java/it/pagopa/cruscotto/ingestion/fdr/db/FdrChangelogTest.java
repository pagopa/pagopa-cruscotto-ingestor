package it.pagopa.cruscotto.ingestion.fdr.db;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ancora la forma dello schema FDR leggendo i changelog Liquibase reali.
 *
 * <p>Non sostituisce una verifica su PostgreSQL vero, e non prova a simularla: qui si controlla
 * cio' che e' <strong>scritto</strong>, non cio' che il database farebbe. E' pero' esattamente il
 * livello a cui vivono i difetti che questo progetto ha gia' pagato — un nome di partizione non
 * citato, un indice che collide, una FK che non doveva esserci — e sono difetti che compaiono solo
 * al rilascio, quando la migration gira.</p>
 *
 * <p>Le garanzie piu' importanti sono quelle <em>negative</em>: le cose che il modello cliente
 * conteneva e che sono state tolte di proposito. Un test che asserisce un'assenza e' l'unico modo
 * perche' qualcuno, rileggendo il SQL del cliente fra sei mesi, non le "ripristini" credendo a una
 * dimenticanza.</p>
 */
class FdrChangelogTest {

    private static final String BUSINESS = "2.3.0.fdr/01_fdr_business_schema.xml";
    private static final String OPERATIONAL = "2.3.0.fdr/02_fdr_operational_schema.xml";

    /** La quarta tabella operativa, introdotta quando il cliente ha escluso lo staging business. */
    private static final String FILE_LOG = "2.3.0.fdr/07_fdr_ingestion_file.xml";

    /** La separazione dalle strutture operative dell'ingestione ADX: via lo staging, dentro il lease. */
    private static final String DECOUPLING = "2.3.0.fdr/08_fdr_operational_decoupling.xml";

    /** La colonna che registra quando ABBIAMO scritto la riga, aggiunta alle due business. */
    private static final String INGESTED_AT = "2.3.0.fdr/09_fdr_business_ingested_at.xml";

    /** Il master della release che introduce FDR. */
    private static final String MASTER = "db.changelog-master-2.3.0.xml";

    /** Il master precedente, che deve restare esattamente com'era. */
    private static final String PREVIOUS_MASTER = "db.changelog-master-2.0.0.xml";

    /** Limite di PostgreSQL per un identificatore: oltre, il nome viene troncato in silenzio. */
    private static final int PG_IDENTIFIER_LIMIT = 63;

    // ─── Accesso ai file ─────────────────────────────────────────────────────────

    /**
     * I changelog <strong>sorgenti</strong>, non la loro copia in {@code target/classes}.
     *
     * <p>La distinzione non e' pedanteria: Maven copia le risorse ma non cancella quelle rimosse
     * senza un {@code clean}, quindi una directory spostata sopravvive nel build output. Leggendo
     * da li', un test che verifica "la vecchia collocazione non esiste piu'" fallirebbe su un
     * artefatto, e — peggio — il controllo delle collisioni di nome troverebbe ogni indice due
     * volte e segnalerebbe conflitti inesistenti. E' successo davvero, scrivendo questo test.</p>
     */
    private static Path changelogDir() {
        Path root = projectRoot();
        Path dir = root.resolve("src/main/resources/db/migration/liquibase/changelog");
        assertThat(dir).as("directory dei changelog non trovata in %s", dir).exists();
        return dir;
    }

    /** Risale dalla posizione nota sul classpath fino alla radice del progetto (il {@code pom.xml}). */
    private static Path projectRoot() {
        URL marker = FdrChangelogTest.class.getClassLoader().getResource("application.yml");
        assertThat(marker).as("application.yml non trovato sul classpath").isNotNull();
        Path path;
        try {
            path = Path.of(marker.toURI());
        } catch (Exception e) {
            throw new IllegalStateException("impossibile risolvere il classpath", e);
        }
        while (path != null && !Files.exists(path.resolve("pom.xml"))) {
            path = path.getParent();
        }
        assertThat(path).as("radice del progetto non trovata").isNotNull();
        return path;
    }

    private static String read(String relative) {
        try {
            return Files.readString(changelogDir().resolve(relative), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("changelog non leggibile: " + relative, e);
        }
    }

    private static String readFromClasspath(String resource) {
        URL url = FdrChangelogTest.class.getClassLoader().getResource(resource);
        assertThat(url).as("risorsa non trovata sul classpath: %s", resource).isNotNull();
        try {
            return Files.readString(Path.of(url.toURI()), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("risorsa non leggibile: " + resource, e);
        }
    }

    /** Ogni changelog del progetto, FDR compresi. */
    private static Map<Path, String> everyChangelog() {
        Map<Path, String> all = new LinkedHashMap<>();
        try (Stream<Path> files = Files.walk(changelogDir())) {
            files.filter(p -> p.toString().endsWith(".xml")).forEach(p -> {
                try {
                    all.put(p, Files.readString(p, StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertThat(all).as("nessun changelog letto").isNotEmpty();
        return all;
    }

    private static String business() {
        return read(BUSINESS);
    }

    private static String operational() {
        return read(OPERATIONAL);
    }

    private static String fileLog() {
        return read(FILE_LOG);
    }

    /**
     * Tutto il DDL FDR, con i commenti XML rimossi.
     *
     * <p>Serve alle asserzioni di assenza: questi file <em>parlano</em> diffusamente di foreign key
     * e vincoli di unicita' per spiegare perche' non ci sono. Cercare "FOREIGN KEY" nel testo grezzo
     * troverebbe la spiegazione e non la cosa, e il test direbbe il falso.</p>
     */
    private static String fdrDdl() {
        return stripComments(business()) + "\n" + stripComments(operational())
                + "\n" + stripComments(fileLog());
    }

    private static String stripComments(String xml) {
        return xml.replaceAll("(?s)<!--.*?-->", " ");
    }

    // ─── Versioning: FDR appartiene alla catena 2.3.x ────────────────────────────

    /**
     * Tutti i changelog FDR presenti sul disco devono essere inclusi nel master, e nell'ordine del
     * nome.
     *
     * <p>Elencarli a mano qui sarebbe la versione fragile dello stesso controllo: un changeSet
     * nuovo e non incluso passerebbe inosservato, ed e' proprio il difetto che conta — il file
     * esiste, i test sul suo contenuto passano, ma nessun ambiente lo applica. La lista viene
     * quindi dalla directory, non dal test.</p>
     */
    @Test
    void ilMasterIncludeTuttiIChangelogFdrNellOrdineGiusto() {
        String master = read(MASTER);
        List<String> onDisk = fdrChangelogNames();

        assertThat(onDisk).as("nessun changelog FDR trovato").isNotEmpty();

        int previous = -1;
        for (String name : onDisk) {
            int at = master.indexOf("2.3.0.fdr/" + name);
            assertThat(at)
                    .as("changeSet presente su disco ma non incluso nel master: %s", name)
                    .isGreaterThan(0);
            assertThat(at)
                    .as("gli include non seguono l'ordine dei nomi: %s", name)
                    .isGreaterThan(previous);
            previous = at;
        }
    }

    /**
     * I master di questo repository sono <strong>cumulativi</strong>: la 2.0.0 include la 1.0.0, e
     * la 2.3.0 deve includere la 2.0.0. Non e' una scelta estetica — a runtime e nel workflow di
     * migration e' attivo <em>un solo</em> master per volta, quindi se non include i precedenti un
     * database vuoto non viene provvisionato.
     */
    @Test
    void ilMasterDiRilascioEcumulativoEIncludeIlPrecedente() {
        String master = read(MASTER);

        assertThat(master).contains(PREVIOUS_MASTER);
        assertThat(master.indexOf(PREVIOUS_MASTER))
                .as("il master precedente deve essere incluso PRIMA dei changeSet FDR")
                .isLessThan(master.indexOf("2.3.0.fdr/"));
    }

    /**
     * Il collegamento che rende reale tutto il resto: {@code LiquibaseConfiguration} costruisce il
     * path del master da {@code application.liquibase.dbVersion}. Se quella property restasse a
     * 2.0.0, il master 2.3.0 sarebbe un file che nessuno apre e i changeSet FDR non verrebbero
     * applicati mai — senza alcun errore, perche' non c'e' nulla che fallisca.
     */
    @Test
    void lApplicazionePuntaAlMasterDellaReleaseCheIntroduceFdr() {
        String applicationYaml = readFromClasspath("application.yml");

        assertThat(applicationYaml)
                .as("dbVersion deve selezionare il master che include FDR")
                .contains("dbVersion: 2.3.0");
        assertThat(changelogDir().resolve(MASTER))
                .as("il master indicato da dbVersion deve esistere")
                .exists();
    }

    /**
     * La vecchia collocazione non deve sopravvivere in nessuna forma: ne' come directory, ne' come
     * include dimenticato. Due catene FDR contemporanee significherebbero gli stessi oggetti creati
     * due volte, con changeSet ID diversi — un fallimento al rilascio, non in sviluppo.
     */
    @Test
    void laVecchiaCollocazione200FdrNonEsistePiu() {
        assertThat(changelogDir().resolve("2.0.0.fdr"))
                .as("la directory 2.0.0.fdr deve essere stata spostata, non duplicata")
                .doesNotExist();

        everyChangelog().forEach((path, body) ->
                assertThat(body)
                        .as("riferimento residuo a 2.0.0.fdr in %s", path.getFileName())
                        .doesNotContain("2.0.0.fdr"));
    }

    /**
     * Nessun include duplicato in tutta la catena: un changeSet incluso due volte verrebbe applicato
     * due volte sotto path diversi, e per le CREATE TABLE significa fallire.
     */
    @Test
    void nessunIncludeDuplicatoNellaCatena() {
        for (String master : List.of(MASTER, PREVIOUS_MASTER, "db.changelog-master-1.0.0.xml")) {
            String body = read(master);
            List<String> includes = new ArrayList<>();
            Matcher matcher = Pattern.compile("<include file=\"([^\"]+)\"").matcher(body);
            while (matcher.find()) {
                includes.add(matcher.group(1));
            }
            assertThat(includes)
                    .as("include duplicati in %s", master)
                    .doesNotHaveDuplicates();
        }
    }

    /**
     * Il master precedente deve restare <strong>esattamente</strong> com'era: nessun include FDR
     * aggiunto, tutti i suoi al loro posto e nello stesso ordine. Toccarlo significherebbe cambiare
     * il checksum di migration gia' applicate in dev e uat, dove lo schema non viene ricreato.
     */
    @Test
    void ilMasterPrecedenteRestaIntatto() {
        String previous = read(PREVIOUS_MASTER);

        assertThat(previous).as("FDR non deve comparire nel master della 2.0.0").doesNotContain("fdr");
        assertThat(previous).contains("db.changelog-master-1.0.0.xml");

        int at = -1;
        for (int i = 1; i <= 9; i++) {
            int found = previous.indexOf("2.0.0.massive.search/0" + i + "_");
            assertThat(found).as("include massive search 0%d rimosso", i).isGreaterThan(0);
            assertThat(found).as("include massive search riordinati").isGreaterThan(at);
            at = found;
        }
    }

    // ─── Evoluzione sicura dei changeSet gia' applicati ──────────────────────────

    /**
     * La documentazione di ogni changelog FDR deve stare <strong>prima</strong> del tag
     * {@code <changeSet>}, non dentro.
     *
     * <p>Non e' una questione di stile: un changeSet applicato non si puo' piu' toccare nel
     * contenuto, perche' cambierebbe il checksum e Liquibase si fermerebbe sugli ambienti dove lo
     * schema non viene ricreato. I commenti XML esterni al changeSet non ne fanno parte, quindi
     * sono l'unico posto dove una spiegazione resta <em>correggibile</em> quando una decisione
     * successiva la supera.</p>
     *
     * <p>E succede: le lunghezze documentate nel 01 sono state poi corrette dal 03 e dal 04, e
     * senza questa proprieta' il file sarebbe rimasto a raccontare un modello che non esiste
     * piu'.</p>
     */
    @Test
    void laDocumentazioneStaFuoriDalChangeSetEquindiRestaCorreggibile() {
        for (String name : fdrChangelogNames()) {
            String body = read("2.3.0.fdr/" + name);

            int documentation = body.indexOf("<!--");
            int changeSet = body.indexOf("<changeSet");
            assertThat(documentation)
                    .as("%s non ha un blocco di documentazione", name)
                    .isGreaterThan(0);
            assertThat(body.indexOf("-->", documentation))
                    .as("in %s la documentazione principale deve chiudersi prima del changeSet,"
                            + " altrimenti correggerla ne cambierebbe il checksum", name)
                    .isLessThan(changeSet);
        }
    }

    /**
     * Nessun oggetto viene creato due volte nella catena FDR.
     *
     * <p>E' il difetto che conta, ed e' asimmetrico: ricreare una tabella, un indice o una sequenza
     * che un changeSet precedente ha gia' creato <strong>passa</strong> su uno schema nuovo e
     * <strong>fallisce</strong> su un ambiente dove il primo e' gia' applicato - cioe' si manifesta
     * in un ambiente su due, e lo si scopre al rilascio.</p>
     *
     * <p>Il test confronta i nomi degli oggetti creati, e non vieta a un changeSet successivo di
     * crearne di <em>nuovi</em>: il 07 introduce {@code FDR_INGESTION_FILE} ed e' legittimo. La
     * formulazione precedente lo vietava, e sarebbe diventata l'ostacolo da rimuovere invece della
     * rete di sicurezza.</p>
     */
    @Test
    void nessunOggettoFdrVieneCreatoDueVolteNellaCatena() {
        List<String> created = new ArrayList<>();
        for (String name : fdrChangelogNames()) {
            String ddl = stripComments(read("2.3.0.fdr/" + name));

            Matcher tables = Pattern.compile(
                            "CREATE TABLE \\$\\{schema_name\\}\\.([A-Za-z0-9_]+)").matcher(ddl);
            while (tables.find()) {
                created.add("table:" + tables.group(1).toUpperCase(Locale.ROOT));
            }
            Matcher sequences = Pattern.compile("sequenceName=\"([A-Za-z0-9_]+)\"").matcher(ddl);
            while (sequences.find()) {
                created.add("sequence:" + sequences.group(1).toUpperCase(Locale.ROOT));
            }
            indexNamesIn(ddl).forEach(index -> created.add("index:" + index));
        }

        assertThat(created).as("nessun oggetto creato: il test non starebbe verificando nulla")
                .isNotEmpty();
        assertThat(created)
                .as("oggetto creato due volte nella catena FDR: fallirebbe dove il primo changeSet"
                        + " e' gia' applicato")
                .doesNotHaveDuplicates();
    }

    /**
     * Solo il 01 crea partizioni: l'orizzonte e il ciclo che le genera stanno li'.
     *
     * <p>Un secondo changeSet che le ricreasse non fallirebbe soltanto - produrrebbe un orizzonte
     * diverso dal primo, cioe' due verita' su quali giornate esistono.</p>
     */
    @Test
    void soloIlPrimoChangeSetCreaPartizioni() {
        for (String name : fdrChangelogNames()) {
            if (name.startsWith("01_")) {
                continue;
            }
            assertThat(stripComments(read("2.3.0.fdr/" + name)))
                    .as("%s crea partizioni", name)
                    .doesNotContain("PARTITION OF");
        }
    }

    /**
     * Nessun rollback puo' accorciare un valore.
     *
     * <p>Restringere un {@code VARCHAR} su dati gia' presenti deve <strong>fallire</strong>, non
     * troncare: il rollback che "funziona sempre" sarebbe quello che distrugge in silenzio proprio
     * i dati che la migration esisteva per salvare. Un {@code USING} con {@code substring} o
     * {@code left} lo renderebbe tale, ed e' la scorciatoia che si prende quando un rollback
     * fallisce e si vuole farlo passare.</p>
     */
    @Test
    void nessunRollbackTroncaIValori() {
        for (String name : fdrChangelogNames()) {
            String body = stripComments(read("2.3.0.fdr/" + name));
            int at = body.indexOf("<rollback>");
            if (at < 0) {
                continue;
            }
            String rollback = body.substring(at).toUpperCase(Locale.ROOT);

            assertThat(rollback).as("%s: rollback con SUBSTRING", name).doesNotContain("SUBSTRING");
            assertThat(rollback).as("%s: rollback con LEFT()", name).doesNotContain("LEFT(");
            assertThat(rollback).as("%s: rollback con USING", name).doesNotContain(" USING ");
            assertThat(rollback).as("%s: clamp nel rollback", name).doesNotContain("CLAMP");
        }
    }

    // ─── Le sei tabelle ──────────────────────────────────────────────────────────

    @Test
    void tutteESeiLeTabelleSonoCreate() {
        String ddl = fdrDdl();

        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FDR_FLUSSI_RENDICONTAZIONE (");
        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FDR_IUV_RENDICONTATI (");
        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FDR_INGESTION_BATCH (");
        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FDR_INGESTION_STATUS (");
        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FDR_INGESTION_EVENT (");
        assertThat(ddl)
                .as("la tabella del log per file manca: il cliente l'ha chiesta al posto dello"
                        + " staging business")
                .contains("CREATE TABLE ${schema_name}.FDR_INGESTION_FILE (");
    }

    @Test
    void unaSequenzaPerTabellaNessunaCondivisa() {
        String ddl = fdrDdl();
        List<String> expected = List.of(
                "SQ_FDR_FLUSSI_RENDICONTAZIONE", "SQ_FDR_IUV_RENDICONTATI",
                "SQ_FDR_INGESTION_BATCH", "SQ_FDR_INGESTION_STATUS", "SQ_FDR_INGESTION_EVENT",
                "SQ_FDR_INGESTION_FILE");

        expected.forEach(sequence ->
                assertThat(ddl).contains("sequenceName=\"" + sequence + "\""));

        long declared = Pattern.compile("<createSequence").matcher(ddl).results().count();
        assertThat(declared)
                .as("una sequenza per tabella: ne' meno (condivisa) ne' di piu'")
                .isEqualTo(expected.size());
    }

    /**
     * Le tabelle business hanno {@code ID} come {@code BIGINT} e non {@code INTEGER} come il modello
     * cliente e come le tabelle ADX dell'AS-IS. Non e' uno stile: FDR_IUV_RENDICONTATI prende una riga
     * per transfer rendicontato, e a milioni al giorno un INTEGER si esaurisce in meno di tre anni.
     */
    @Test
    void leChiaviDelleTabelleBusinessSonoBigint() {
        assertThat(business()).contains("ID                 BIGINT    NOT NULL");
        assertThat(business()).contains("ID                           BIGINT    NOT NULL");
        assertThat(stripComments(business()))
                .as("nessun ID a INTEGER sulle tabelle business")
                .doesNotContain("ID INTEGER");
    }

    // ─── Partizionamento ─────────────────────────────────────────────────────────

    @Test
    void leDueTabelleBusinessSonoPartizionatePerGiornoSuDateInserted() {
        String ddl = business();

        assertThat(countOf(ddl, "PARTITION BY RANGE (DATE_INSERTED)"))
                .as("entrambe le tabelle business devono essere partizionate su DATE_INSERTED")
                .isEqualTo(2);
        assertThat(ddl).contains("d + INTERVAL '1 day'");
        assertThat(ddl).contains("to_char(d, 'YYYYMMDD')");
    }

    /**
     * La primary key deve cominciare dalla chiave di partizionamento. Su una tabella partizionata la
     * chiave di partizionamento <em>deve</em> farne parte; metterla per prima fa in modo che lo
     * stesso indice serva anche gli accessi per giornata, che sono quelli della reingestion.
     */
    @Test
    void laPrimaryKeyBusinessIniziaDallaChiaveDiPartizionamento() {
        String ddl = business();

        assertThat(ddl).contains(
                "CONSTRAINT FDR_FLUSSI_RENDICONTAZIONE_PK PRIMARY KEY (DATE_INSERTED, ID)");
        assertThat(ddl).contains(
                "CONSTRAINT FDR_IUV_RENDICONTATI_PK PRIMARY KEY (DATE_INSERTED, ID)");
    }

    @Test
    void entrambeLeTabelleBusinessHannoLaPartizioneDefault() {
        String ddl = business();

        assertThat(ddl).contains(
                "CREATE TABLE ${schema_name}.FDR_FLUSSI_RENDICONTAZIONE_DEFAULT");
        assertThat(ddl).contains(
                "CREATE TABLE ${schema_name}.FDR_IUV_RENDICONTATI_DEFAULT");
        assertThat(countOf(ddl, "PARTITION OF ${schema_name}.FDR_FLUSSI_RENDICONTAZIONE DEFAULT"))
                .isEqualTo(1);
        assertThat(countOf(ddl, "PARTITION OF ${schema_name}.FDR_IUV_RENDICONTATI DEFAULT"))
                .isEqualTo(1);
    }

    /**
     * Il nome delle partizioni e' maiuscolo, quindi DEVE essere citato. {@code format('%I', ...)}
     * cita l'identificatore; concatenarlo nella stringa no, e PostgreSQL lo abbasserebbe a
     * minuscolo. La conseguenza non e' un errore all'applicazione della migration — le partizioni
     * nascerebbero comunque, solo con un altro nome — ma un TRUNCATE della reingestion che non
     * trova la partizione. E' il difetto che ha tenuto rotto per mesi il job di retention dello
     * staging, e che si manifesta solo molto dopo.
     */
    @Test
    void iNomiDellePartizioniSonoCitatiConFormatI() {
        String ddl = business();

        assertThat(countOf(ddl, "CREATE TABLE ${schema_name}.%I PARTITION OF"))
                .as("entrambe le tabelle devono creare le partizioni con %I")
                .isEqualTo(2);
        assertThat(stripComments(ddl))
                .as("nessuna concatenazione diretta del nome di partizione nella CREATE TABLE")
                .doesNotContain("CREATE TABLE ${schema_name}.' ||");
    }

    /**
     * L'orizzonte e' allineato a quello gia' usato dall'AS-IS per le partizioni giornaliere. Non e'
     * una decisione di retention: serve a far si' che quando andra' esteso — e oggi nulla lo
     * estende — lo si estenda per tutte le tabelle insieme.
     */
    @Test
    void lOrizzonteDellePartizioniEallineatoAllAsIs() {
        String ddl = business();

        assertThat(ddl).contains("DECLARE d DATE := '2026-01-01'");
        assertThat(ddl).contains("WHILE d &lt; '2028-01-01' LOOP");
    }

    /**
     * I nomi generati dal ciclo devono stare sotto il limite di PostgreSQL, contando anche il
     * suffisso che il database aggiunge da se' all'indice di primary key di ogni partizione.
     */
    /**
     * Il nome piu' lungo che questo schema produce non e' quello della partizione: e' quello
     * dell'<strong>indice propagato</strong> su di essa, che PostgreSQL costruisce da solo come
     * {@code <partizione>_<colonne>_idx}. Oltre i 63 caratteri non fallisce — tronca e aggiunge un
     * progressivo per disambiguare — ma il nome diventa imprevedibile, e un indice che non si
     * riesce a nominare e' un indice che non si riesce a cercare quando serve.
     *
     * <p>Il margine conta davvero qui: il prefisso {@code FDR_} ne ha consumato quattro caratteri,
     * e la colonna piu' lunga indicizzata, {@code DATA_ESITO_SINGOLO_PAGAMENTO}, ne porta 28. Il
     * caso peggiore e' quindi a un passo dal limite. Il test lo ricava dal DDL invece di scriverlo
     * a mano, cosi' indicizzare domani una colonna piu' lunga lo fa fallire subito.</p>
     */
    @Test
    void iNomiGeneratiDaPostgresStannoSottoIlLimite() {
        String ddl = stripComments(business());

        for (String table : List.of("FDR_FLUSSI_RENDICONTAZIONE", "FDR_IUV_RENDICONTATI")) {
            String partition = table + "_20260101";
            assertThat(partition.length() + "_pkey".length())
                    .as("nome dell'indice di primary key della partizione %s", partition)
                    .isLessThanOrEqualTo(PG_IDENTIFIER_LIMIT);

            // La colonna piu' lunga fra quelle indicizzate su questa tabella, presa dal DDL.
            Matcher indexes = Pattern.compile(
                            "CREATE INDEX\\s+\\w+\\s+ON\\s+\\$\\{schema_name\\}\\." + table + "\\s*\\(([^)]+)\\)")
                    .matcher(ddl);
            int longestColumns = 0;
            String worst = "";
            while (indexes.find()) {
                String columns = indexes.group(1).replaceAll("[\\s,]+", "_");
                if (columns.length() > longestColumns) {
                    longestColumns = columns.length();
                    worst = columns;
                }
            }
            assertThat(longestColumns).as("nessun indice trovato su %s", table).isPositive();

            String generated = partition + "_" + worst.toLowerCase(Locale.ROOT) + "_idx";
            assertThat(generated.length())
                    .as("PostgreSQL troncherebbe questo nome, rendendolo imprevedibile: %s (%d)",
                            generated, generated.length())
                    .isLessThanOrEqualTo(PG_IDENTIFIER_LIMIT);
        }
    }

    // ─── Garanzie negative: cio' che il modello cliente aveva e che e' stato tolto ─

    /**
     * Il modello cliente dichiara {@code ALTER TABLE "FDR_IUV_RENDICONTATI" ADD CONSTRAINT ...
     * FOREIGN KEY ("UNIQUE_ID")}. E' ignorato deliberatamente: la relazione revisione -> transfer e'
     * LOGICA. Con la FK fisica, un transfer la cui revisione non e' ancora arrivata — o non
     * arrivera' mai, perche' il sorgente e' incoerente — non sarebbe acquisibile, che e' esattamente
     * cio' che il requisito vieta.
     */
    @Test
    void nessunaForeignKeySuUniqueId() {
        assertThat(fdrDdl().toUpperCase(Locale.ROOT))
                .as("UNIQUE_ID deve restare una relazione logica")
                .doesNotContain("FOREIGN KEY (UNIQUE_ID)");
    }

    /**
     * {@code UNIQUE_ID} e' indicizzato ma NON unique: la stessa revisione compare su tutti i suoi
     * transfer, quindi una UNIQUE li rifiuterebbe tutti tranne il primo.
     */
    @Test
    void uniqueIdEindicizzatoMaNonUnique() {
        String ddl = fdrDdl();

        assertThat(ddl).contains("IDX_FDR_FLUSSI_UNIQUE_ID");
        assertThat(ddl).contains("IDX_FDR_IUV_UNIQUE_ID");
        assertThat(ddl).doesNotContain("CREATE UNIQUE INDEX");
        assertThat(ddl.toUpperCase(Locale.ROOT)).doesNotContain("UNIQUE (UNIQUE_ID)");
    }

    /**
     * Il transfer e' funzionalmente (IUV, IUR, IDSP), ma e' la chiave con cui si LEGGE il dato, non
     * un vincolo di scrittura. I sample reali mostrano gia' la stessa coppia (IUV, IUR) su flussi e
     * revisioni diversi, e le anomalie del sorgente devono poter entrare.
     */
    @Test
    void nessunVincoloDiUnicitaSulTransfer() {
        String ddl = fdrDdl().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");

        assertThat(ddl).doesNotContain("UNIQUE (IUV");
        assertThat(ddl).doesNotContain("UNIQUE (IUR");
        assertThat(ddl).doesNotContain("IUV, IUR, IDSP");
    }

    /**
     * Nessun CHECK e nessun NOT NULL sulle colonne che arrivano dal sorgente: sulle due tabelle
     * business le sole NOT NULL ammesse sono ID e DATE_INSERTED, che valorizziamo noi e che formano
     * la primary key. Ogni altro vincolo sarebbe un modo di rifiutare una riga anomala invece di
     * acquisirla.
     */
    @Test
    void leTabelleBusinessNonVincolanoIlDatoSorgente() {
        assertThat(stripComments(business()))
                .as("nessun CHECK sulle tabelle business")
                .doesNotContain("CHECK (");

        // Il conteggio e' ristretto al CORPO di ciascuna tabella, non all'intero file: contarlo su
        // tutto il changelog includerebbe indici, ciclo delle partizioni e rollback, e un NOT NULL
        // aggiunto li' domani farebbe fallire questo test per la ragione sbagliata.
        for (String table : List.of("FDR_FLUSSI_RENDICONTAZIONE", "FDR_IUV_RENDICONTATI")) {
            String body = tableBody(table);
            long notNulls = Pattern.compile("NOT NULL").matcher(body).results().count();

            assertThat(notNulls)
                    .as("su %s solo ID e DATE_INSERTED possono essere NOT NULL: ogni altro vincolo"
                            + " rifiuterebbe una riga anomala invece di acquisirla", table)
                    .isEqualTo(2);
            // Normalizzati: l'allineamento delle colonne differisce fra le due tabelle e non e'
            // cio' che questo test deve proteggere.
            assertThat(columnType(body, "ID").replaceAll("\\s+", " ")).isEqualTo("BIGINT NOT NULL");
            assertThat(columnType(body, "DATE_INSERTED").replaceAll("\\s+", " "))
                    .isEqualTo("DATE NOT NULL");
        }
    }

    // ─── Lunghezze: baseline provvisoria, in attesa di conferma del cliente ──────

    /**
     * Le lunghezze sono quelle del modello cliente ovunque i dati reali non le smentiscano, e tutte
     * ormai <strong>confermate dal cliente</strong>: {@code ID_FLUSSO = 50} sostituisce la sua
     * proposta originale di 30, che i dati smentivano (osservati 35), e {@code UNIQUE_ID = 36}
     * sostituisce sia il suo 32 originale — incompatibile con gli UUID osservati — sia la baseline
     * provvisoria di 64 che avevamo adottato in attesa.
     *
     * <p>Verifica il tipo <em>effettivo</em>, quindi tiene conto delle correzioni fatte con un
     * changeSet successivo: e' cio' che la colonna avra' davvero.
     */
    @Test
    void leLunghezzeDiFlussiRendicontazioneSonoQuelleConcordate() {
        assertThat(effectiveColumnType("FDR_FLUSSI_RENDICONTAZIONE", "ID_FLUSSO")).isEqualTo("VARCHAR(50)");
        assertThat(effectiveColumnType("FDR_FLUSSI_RENDICONTAZIONE", "CAUSALE")).isEqualTo("VARCHAR(30)");
        assertThat(effectiveColumnType("FDR_FLUSSI_RENDICONTAZIONE", "ID_DOMINIO")).isEqualTo("VARCHAR(11)");
        assertThat(effectiveColumnType("FDR_FLUSSI_RENDICONTAZIONE", "PSP")).isEqualTo("VARCHAR(15)");
        assertThat(effectiveColumnType("FDR_FLUSSI_RENDICONTAZIONE", "INT_PSP")).isEqualTo("VARCHAR(35)");
        assertThat(effectiveColumnType("FDR_FLUSSI_RENDICONTAZIONE", "UNIQUE_ID")).isEqualTo("VARCHAR(36)");
    }

    @Test
    void leLunghezzeDiIuvRendicontatiSonoQuelleConcordate() {
        assertThat(effectiveColumnType("FDR_IUV_RENDICONTATI", "IUV")).isEqualTo("VARCHAR(35)");
        assertThat(effectiveColumnType("FDR_IUV_RENDICONTATI", "IUR")).isEqualTo("VARCHAR(35)");
        assertThat(effectiveColumnType("FDR_IUV_RENDICONTATI", "ID_FLUSSO")).isEqualTo("VARCHAR(50)");
        assertThat(effectiveColumnType("FDR_IUV_RENDICONTATI", "ID_DOMINIO")).isEqualTo("VARCHAR(11)");
        assertThat(effectiveColumnType("FDR_IUV_RENDICONTATI", "PSP")).isEqualTo("VARCHAR(15)");
        assertThat(effectiveColumnType("FDR_IUV_RENDICONTATI", "INT_PSP")).isEqualTo("VARCHAR(35)");
        assertThat(effectiveColumnType("FDR_IUV_RENDICONTATI", "UNIQUE_ID")).isEqualTo("VARCHAR(36)");
    }

    /**
     * {@code INT_PSP} non deve tornare ai limiti che i dati reali hanno dimostrato insufficienti.
     *
     * <p>I valori vietati sono due, e la distinzione conta. L'11 e' quello del modello cliente,
     * smentito da 4.185 righe. Il 14 e' il <em>massimo esatto osservato</em>, ed e' vietato per una
     * ragione meno ovvia: dimensionare di nuovo sul massimo di una sola giornata ripeterebbe
     * l'errore che ci ha portati qui, visto che anche l'11 era "il massimo osservato" su un
     * campione piu' piccolo. Un identificativo di intermediario accorciato, poi, non
     * identificherebbe piu' nulla: qui troncare non e' un'opzione.</p>
     *
     * <p><strong>CAUSALE non e' piu' in questo test, ed e' la parte che conta.</strong> Fino al
     * changeSet 04 asseriva che non potesse tornare a 30 — cioe' esattamente cio' che il cliente ha
     * poi deciso. Un test scritto su una decisione invece che sul perche' la irrigidisce, e quando
     * la decisione cambia diventa l'ostacolo da rimuovere anziche' la rete di sicurezza. La
     * garanzia su CAUSALE vive ora in {@link #laCausaleRestaA30ConTroncamentoEsplicito()}, che
     * protegge il senso opposto.</p>
     */
    @Test
    void intPspNonTornaAiLimitiSmentitiDaiDatiReali() {
        for (String table : List.of("FDR_FLUSSI_RENDICONTAZIONE", "FDR_IUV_RENDICONTATI")) {
            String intPsp = effectiveColumnType(table, "INT_PSP");
            assertThat(intPsp)
                    .as("%s.INT_PSP e' tornata a 11: non reggerebbe un intermediario estero", table)
                    .isNotEqualTo("VARCHAR(11)");
            assertThat(intPsp)
                    .as("%s.INT_PSP dimensionata sul massimo esatto osservato", table)
                    .isNotEqualTo("VARCHAR(14)");
        }
    }

    /**
     * {@code CAUSALE} resta a 30 per decisione del cliente, e il troncamento che ne consegue deve
     * restare documentato accanto allo schema.
     *
     * <p>Il rischio da cui protegge non e' teorico: i dati dicono che 40.570 righe su 186.857 — il
     * 21,71% — superano i 30 caratteri, quindi chiunque rilegga quell'evidenza senza conoscere la
     * decisione concludera' che il 30 e' un difetto e lo allarghera'. E' gia' successo una volta,
     * con il changeSet 04.</p>
     *
     * <p>Il test verifica quindi due cose insieme: la lunghezza, e che la ragione sia scritta dove
     * la si incontra. Senza la seconda la prima verrebbe "corretta" in buona fede.</p>
     */
    @Test
    void laCausaleRestaA30ConTroncamentoEsplicito() {
        assertThat(effectiveColumnType("FDR_FLUSSI_RENDICONTAZIONE", "CAUSALE"))
                .as("CAUSALE allargata di nuovo: il cliente ha scelto 30 e il troncamento")
                .isEqualTo("VARCHAR(30)");

        String changelogs = String.join("\n", fdrChangelogs());
        assertThat(changelogs)
                .as("la decisione del cliente su CAUSALE non e' documentata nei changelog")
                .contains("DECISIONE DEL CLIENTE");
        assertThat(changelogs)
                .as("l'obbligo di troncare CAUSALE deve essere esplicito, altrimenti il 30"
                        + " sembrera' un difetto a chi legge i dati reali")
                .containsIgnoringCase("TRONCARE");
    }

    /**
     * {@code UNIQUE_ID} non deve tornare a 32: e' il valore della proposta originale del cliente, e
     * gli UUID osservati nei dati reali ne hanno 36. Ripristinarlo leggendo quel documento
     * rifiuterebbe <strong>ogni</strong> riga, non qualcuna.
     */
    @Test
    void uniqueIdNonTornaAllaLunghezzaCheRifiuterebbeOgniUuid() {
        for (String table : List.of("FDR_FLUSSI_RENDICONTAZIONE", "FDR_IUV_RENDICONTATI")) {
            assertThat(effectiveColumnType(table, "UNIQUE_ID"))
                    .as("%s.UNIQUE_ID non puo' essere piu' corto di un UUID", table)
                    .isNotEqualTo("VARCHAR(32)");
        }
    }

    /**
     * La correzione deve toccare <strong>entrambe</strong> le tabelle. Applicarla a una sola
     * renderebbe la stessa revisione accettabile da una parte e rifiutata dall'altra, proprio sulla
     * colonna che le lega — ed e' un errore facile da fare, perche' l'ALTER va scritto due volte.
     */
    @Test
    void laCorrezioneDiUniqueIdEapplicataAEntrambeLeTabelle() {
        String altered = fdrChangelogs().stream()
                .map(FdrChangelogTest::stripComments)
                .reduce("", (a, b) -> a + " " + b)
                .replaceAll("\\s+", " ");

        assertThat(countOf(altered, "ALTER COLUMN UNIQUE_ID TYPE VARCHAR(36)"))
                .as("un ALTER per tabella nel forward, piu' nessuno: il rollback usa VARCHAR(64)")
                .isEqualTo(2);
    }

    /**
     * {@code ID_FLUSSO} ha gia' avuto tre valori: 30 (proposta cliente, smentita dai dati), 64
     * (nostra baseline in attesa di risposta) e 50 (la conferma). Una colonna con questa storia e'
     * esattamente quella su cui si torna indietro per sbaglio leggendo un documento vecchio: le
     * asserzioni negative nominano i due valori superati perche' il fallimento dica <em>quale</em>
     * regressione e' avvenuta, non solo che qualcosa non torna.
     */
    @Test
    void idFlussoNonTornaAiValoriSuperati() {
        for (String table : List.of("FDR_FLUSSI_RENDICONTAZIONE", "FDR_IUV_RENDICONTATI")) {
            String type = columnType(tableBody(table), "ID_FLUSSO");

            assertThat(type)
                    .as("%s.ID_FLUSSO e' tornato alla proposta cliente originale, che i sample"
                            + " smentiscono (osservati 35 caratteri)", table)
                    .isNotEqualTo("VARCHAR(30)");
            assertThat(type)
                    .as("%s.ID_FLUSSO e' tornato alla baseline provvisoria, superata dalla conferma"
                            + " del cliente", table)
                    .isNotEqualTo("VARCHAR(64)");
            assertThat(type)
                    .as("%s.ID_FLUSSO e' tornato al VARCHAR(255) generalizzato", table)
                    .isNotEqualTo("VARCHAR(255)");
        }
    }

    /**
     * Le colonne duplicate per denormalizzazione devono avere lo stesso tipo nelle due tabelle: se
     * divergessero, un valore accettato da una sarebbe rifiutato dall'altra e la revisione e i suoi
     * transfer smetterebbero di essere confrontabili proprio sulla chiave che li lega.
     */
    @Test
    void leColonneDenormalizzateHannoLoStessoTipoNelleDueTabelle() {
        String flussi = tableBody("FDR_FLUSSI_RENDICONTAZIONE");
        String iuv = tableBody("FDR_IUV_RENDICONTATI");

        for (String column : List.of("ID_FLUSSO", "ID_DOMINIO", "PSP", "INT_PSP", "UNIQUE_ID",
                "DATA_ORA_FLUSSO", "INSERTED_TIMESTAMP")) {
            assertThat(effectiveColumnType("FDR_IUV_RENDICONTATI", column))
                    .as("tipo divergente per %s fra le due tabelle", column)
                    .isEqualTo(effectiveColumnType("FDR_FLUSSI_RENDICONTAZIONE", column));
        }
    }

    /**
     * Verifica negativa esplicita. Il VARCHAR(255) uniforme era la scelta precedente, ed e' il
     * valore verso cui si ricade naturalmente quando non si sa che lunghezza mettere: rinuncia a
     * controllare qualunque cosa anche dove il tracciato e' plausibile. Questo test fa in modo che
     * tornarci sia una decisione, non una distrazione.
     */
    @Test
    void nessunaColonnaBusinessTornaAVarchar255() {
        assertThat(stripComments(business()))
                .as("VARCHAR(255) reintrodotto su una tabella business")
                .doesNotContain("VARCHAR(255)");
    }

    /**
     * Lo schema non accorcia mai nulla da solo: nessun {@code substring}, nessun {@code LEFT},
     * nessun clamp nel DDL. Un valore troppo lungo deve far fallire l'insert, perche' e' il segnale
     * che il tracciato e' cambiato, e accorciarlo lo nasconderebbe.
     *
     * <p>L'unica eccezione, {@code CAUSALE}, non contraddice questo test: il cliente ha chiesto di
     * troncarla, ma il troncamento avviene <strong>nell'applicazione</strong> in fase di
     * ingestione, non nel DDL. La colonna resta a 30 e il database continua a rifiutare un valore
     * piu' lungo — e' chi scrive a doverlo accorciare prima, consapevolmente.</p>
     */
    @Test
    void loSchemaNonAccorciaMaiUnValore() {
        // Le asserzioni negative vanno sul DDL e non sul testo grezzo: il commento del changeSet
        // *parla* di clamp e substring per vietarli, e cercarli nel testo troverebbe il divieto
        // invece della cosa vietata.
        String ddl = stripComments(business());

        assertThat(ddl).doesNotContain("substring");
        assertThat(ddl).doesNotContain("LEFT(");
        assertThat(ddl).doesNotContain("ColumnValueClamp");
        assertThat(business())
                .as("il divieto di troncamento deve restare scritto dove si leggono le lunghezze")
                .contains("NESSUN CLAMP, NESSUN TRONCAMENTO");
    }

    // ─── Semantica dei timestamp ─────────────────────────────────────────────────

    /**
     * La semantica consolidata deve restare scritta accanto allo schema che la sostiene: sono
     * <strong>entrambe</strong> colonne del dato, non della nostra elaborazione, e una reingestion
     * le riproduce identiche. Se le ridatasse a oggi, il TRUNCATE della partizione della giornata
     * sposterebbe il dato invece di sostituirlo — e lo schema, da solo, non puo' impedirlo.
     */
    @Test
    void laSemanticaDeiTimestampEdocumentataAccantoAlloSchema() {
        String ddl = business();

        assertThat(ddl).contains("DATE_INSERTED / INSERTED_TIMESTAMP - SEMANTICA CONSOLIDATA");
        assertThat(ddl).contains("LA REINGESTION RICOSTRUISCE IL DATO, NON LO RIDATA");
        assertThat(ddl).contains("LocalDate.now()");
    }

    /**
     * Lo schema non deve valorizzare da se' ne' la giornata ne' il timestamp: lo fa l'applicazione,
     * riproducendo il dato originale. Un DEFAULT o un trigger li riscriverebbe alla reingestion,
     * che e' precisamente il difetto da evitare.
     */
    @Test
    void loSchemaNonDataLeRigheDaSolo() {
        String ddl = stripComments(business());

        assertThat(ddl).doesNotContain("CREATE TRIGGER");
        assertThat(ddl).doesNotContain("GENERATED ALWAYS AS");
        assertThat(ddl).doesNotContain("DEFAULT CURRENT_TIMESTAMP");
        assertThat(ddl).doesNotContain("DEFAULT now()");
    }

    /** Mai floating point su un importo, e nessuna scala inventata che arrotonderebbe in silenzio. */
    @Test
    void gliImportiSonoNumericSenzaScalaInventata() {
        String ddl = stripComments(business());

        assertThat(ddl).contains("SOMMA_VERSATA      NUMERIC,");
        assertThat(ddl).contains("IMPORTO                      NUMERIC,");
        assertThat(ddl).doesNotContain("NUMERIC(");
        assertThat(ddl.toUpperCase(Locale.ROOT)).doesNotContain("DOUBLE");
        assertThat(ddl.toUpperCase(Locale.ROOT)).doesNotContain("FLOAT");
        assertThat(ddl.toUpperCase(Locale.ROOT)).doesNotContain("REAL ");
    }

    /**
     * ALL_DATES e' TEXT. Non JSONB: JSONB valida, quindi un valore malformato farebbe fallire
     * l'INSERT invece di essere acquisito, e normalizza, quindi non conserva il valore ricevuto.
     * Non VARCHAR(n): nessuna lunghezza e' certificata e un array puo' crescere.
     * Nessun indice, nessun constraint: la semantica business e' ancora aperta.
     */
    @Test
    void allDatesEconservatoComeTestoSenzaInterpretazione() {
        String ddl = stripComments(business());

        assertThat(ddl).contains("ALL_DATES          TEXT,");
        assertThat(ddl).doesNotContain("JSONB");
        assertThat(ddl).as("nessun indice su ALL_DATES").doesNotContain("(ALL_DATES)");
    }

    // ─── Tabelle operative ───────────────────────────────────────────────────────

    /**
     * Le FK operative sono ammesse — quelle tabelle sono nostre — ma devono essere esattamente tre,
     * tutte RESTRICT. RESTRICT e non CASCADE perche' una retention FDR non esiste ancora: con
     * CASCADE, la prima pulizia scritta in futuro cancellerebbe in silenzio tutto l'audit.
     */
    @Test
    void leForeignKeyOperativeSonoTreETutteRestrict() {
        String ddl = stripComments(operational());

        assertThat(ddl).contains("CONSTRAINT FK_FDR_STATUS_BATCH FOREIGN KEY (BATCH_ID)");
        assertThat(ddl).contains("CONSTRAINT FK_FDR_EVENT_BATCH FOREIGN KEY (BATCH_ID)");
        assertThat(ddl).contains("CONSTRAINT FK_FDR_EVENT_STATUS FOREIGN KEY (STATUS_ID)");

        long foreignKeys = Pattern.compile("FOREIGN KEY").matcher(ddl).results().count();
        assertThat(foreignKeys).isEqualTo(3);
        assertThat(countOf(ddl, "ON DELETE RESTRICT")).isEqualTo(3);
        assertThat(ddl).as("nessun CASCADE sull'audit").doesNotContain("ON DELETE CASCADE");
    }

    /**
     * Le FK operative non devono toccare le tabelle business: un'anomalia del dato sorgente non deve
     * poter essere bloccata da una tabella di controllo. E' la regola che tiene separati i due
     * livelli.
     */
    @Test
    void nessunaForeignKeyVersoLeTabelleBusiness() {
        String ddl = stripComments(operational());

        assertThat(ddl).doesNotContain("REFERENCES ${schema_name}.FDR_FLUSSI_RENDICONTAZIONE");
        assertThat(ddl).doesNotContain("REFERENCES ${schema_name}.FDR_IUV_RENDICONTATI");
    }

    /**
     * Il riferimento a STG_INGEST_ERROR porta ID <strong>e</strong> CREATED_AT, perche' la primary
     * key di quella tabella e' composta: con il solo ID, ritrovare la riga costringerebbe a sondare
     * tutte le sue partizioni giornaliere. Ed e' un riferimento debole, senza FK, perche' quelle
     * partizioni vengono troncate dalla retention — una FK bloccherebbe la pulizia o cancellerebbe
     * l'audit.
     */
    @Test
    void ilRiferimentoAlloStagingPortaLaChiaveCompletaMaNonEunaForeignKey() {
        String ddl = stripComments(operational());

        assertThat(ddl).contains("STG_ERROR_ID         BIGINT");
        assertThat(ddl).contains("STG_ERROR_CREATED_AT TIMESTAMP WITH TIME ZONE");
        assertThat(ddl).doesNotContain("REFERENCES ${schema_name}.STG_INGEST_ERROR");
    }

    /**
     * Niente UNIQUE(workflow, giorno): impedirebbe di reingerire una giornata, che e' un requisito
     * confermato. L'unicita' che c'e' e' quella della riga dentro il proprio batch — una reingestion
     * e' un batch nuovo, quindi genera righe nuove e la storia resta.
     */
    @Test
    void lUnicitaOperativaNonImpedisceUnaFuturaReingestion() {
        String ddl = stripComments(operational());

        assertThat(ddl).contains(
                "CONSTRAINT UK_FDR_STATUS_BATCH_DAY_DATASET UNIQUE (BATCH_ID, BUSINESS_DAY, DATASET)");
        assertThat(ddl.replaceAll("\\s+", " "))
                .doesNotContain("UNIQUE (WORKFLOW")
                .doesNotContain("UNIQUE (BUSINESS_DAY");
    }

    /**
     * Lo stato del retry deve essere rappresentabile e selezionabile: tentativo corrente, istante
     * del prossimo tentativo, stato. L'indice mette l'uguaglianza prima della disuguaglianza, che e'
     * l'ordine che serve a {@code STATUS = 'WAITING_RETRY' AND NEXT_ATTEMPT_AT <= now}.
     */
    @Test
    void loStatoDelRetryErappresentabileESelezionabile() {
        String ddl = stripComments(operational());

        assertThat(ddl).contains("ATTEMPT_COUNT      INTEGER DEFAULT 0        NOT NULL");
        assertThat(ddl).contains("NEXT_ATTEMPT_AT    TIMESTAMP WITH TIME ZONE");
        assertThat(ddl).contains(
                "ON ${schema_name}.FDR_INGESTION_STATUS (STATUS, NEXT_ATTEMPT_AT)");
    }

    /**
     * I valori 5 e 1h sono configurazione applicativa ({@code fdr.retry.*}), non schema. Nel
     * database sta lo stato necessario a farli rispettare, non la politica: cosi' cambiare la soglia
     * e' una property e non una migration con lock.
     */
    @Test
    void laPoliticaDiRetryNonEcodificataNelDatabase() {
        String ddl = stripComments(operational());

        assertThat(ddl).as("nessun CHECK sul numero di tentativi").doesNotContain("ATTEMPT_COUNT <");
        assertThat(ddl).doesNotContain("ATTEMPT_COUNT <=");
        assertThat(ddl).doesNotContain("INTERVAL '1 hour'");
    }

    /**
     * Nessun CHECK su stati, workflow, dataset e severity. La state machine verra' raffinata da
     * DAILY, BOOTSTRAP, REINGESTION e dal retry: un CHECK fissato ora andrebbe modificato da
     * ciascuno di essi, e su una tabella viva e' una migration con lock. L'AS-IS lo ha gia'
     * sperimentato — CHK_INGEST_EXECUTION_LOG_STATUS ha dovuto essere esteso dalla migration 34 per
     * aggiungere ABORTED.
     */
    @Test
    void laStateMachineNonEirrigiditaDaUnCheck() {
        assertThat(stripComments(operational()))
                .as("la validazione degli stati sta nell'applicazione, non nello schema")
                .doesNotContain("CHECK (");
    }

    /**
     * {@code REQUESTED_FROM} e {@code REQUESTED_TO} devono essere <strong>nullable</strong>.
     *
     * <p>Il changeSet 02 le ha create obbligatorie dando per scontato che una richiesta fosse
     * sempre un intervallo. Una REINGESTION su giornate sparse — 10/10, 15/11, 10/12 — non lo e',
     * e con le colonne obbligatorie l'unica via sarebbe riempirle con il minimo e il massimo:
     * scriverebbe a database che sono state richieste anche le sessanta giornate intermedie, che e'
     * falso e che nessuno, rileggendolo, avrebbe modo di riconoscere come tale.
     *
     * <p>Il test guarda lo schema <em>effettivo</em>, non la sola {@code CREATE TABLE}: e' proprio
     * la differenza che questo changeSet introduce.
     */
    @Test
    void gliEstremiDellaRichiestaSonoFacoltativiPerLeGiornateSparse() {
        for (String column : List.of("REQUESTED_FROM", "REQUESTED_TO")) {
            assertThat(isNotNullInCreateTable(column))
                    .as("%s nasce NOT NULL nel changeSet 02", column)
                    .isTrue();
            assertThat(notNullDropped(column))
                    .as("%s deve essere resa facoltativa da un changeSet successivo: senza, una"
                            + " reingestione su giornate sparse non e' rappresentabile senza"
                            + " scrivere un intervallo che nessuno ha chiesto", column)
                    .isTrue();
        }
    }

    /**
     * Il rollback non inventa un perimetro: o lo ricava dagli status, o cancella.
     *
     * <p>Riempire gli estremi con il minimo e il massimo delle giornate chieste e' ammesso solo
     * li', e solo perche' il rollback deve poter ripristinare il vincolo. Farlo nel percorso in
     * avanti sarebbe esattamente la scorciatoia che questo changeSet esiste per evitare.
     */
    @Test
    void ilPerimetroMinimoMassimoVivesoloNelRollback() {
        String changelog = read("2.3.0.fdr/06_fdr_batch_requested_range_nullable.xml");
        String ddl = stripComments(changelog);
        int rollbackAt = ddl.indexOf("<rollback>");

        assertThat(rollbackAt).as("il changeSet deve dichiarare un rollback").isGreaterThan(0);
        assertThat(ddl.substring(0, rollbackAt))
                .as("nessun min/max nel percorso in avanti")
                .doesNotContain("min(")
                .doesNotContain("max(");
        assertThat(ddl.substring(rollbackAt))
                .as("il rollback ricava il perimetro dagli status, che sono la fonte autorevole")
                .contains("FDR_INGESTION_STATUS");
    }

    /** {@code true} se la colonna nasce obbligatoria nella CREATE TABLE operativa. */
    private static boolean isNotNullInCreateTable(String column) {
        String ddl = stripComments(operational());
        int start = ddl.indexOf("CREATE TABLE ${schema_name}.FDR_INGESTION_BATCH (");
        int end = ddl.indexOf("CONSTRAINT FDR_INGESTION_BATCH_PK", start);
        return Pattern.compile("^\\s*" + column + "\\s+[^,\\n]*NOT NULL", Pattern.MULTILINE)
                .matcher(ddl.substring(start, end))
                .find();
    }

    /** {@code true} se un changeSet successivo rimuove il vincolo, fuori dal proprio rollback. */
    private static boolean notNullDropped(String column) {
        for (String changelog : fdrChangelogs()) {
            String body = stripComments(changelog).replaceAll("\\s+", " ");
            int rollbackAt = body.indexOf("<rollback>");
            String forward = rollbackAt >= 0 ? body.substring(0, rollbackAt) : body;
            if (forward.contains("<dropNotNullConstraint")
                    && forward.contains("columnName=\"" + column + "\"")) {
                return true;
            }
        }
        return false;
    }

    // ─── Il log per file ─────────────────────────────────────────────────────────

    /**
     * Le quattro colonne che il cliente ha chiesto esplicitamente.
     *
     * <p>Sono il requisito, non un dettaglio implementativo: giornata, file, righe del file, righe
     * elaborate. Vengono prima di tutto il resto perche' se una di queste sparisse la tabella non
     * risponderebbe piu' alla domanda per cui esiste.</p>
     */
    @Test
    void leQuattroColonneChiesteDalClienteCiSonoTutte() {
        String body = fileTableBody();

        assertThat(columnType(body, "BUSINESS_DAY").replaceAll("\\s+", " "))
                .as("la giornata di ingestion")
                .isEqualTo("DATE NOT NULL");
        assertThat(columnType(body, "BLOB_NAME").replaceAll("\\s+", " "))
                .as("il file analizzato")
                .isEqualTo("VARCHAR(1024) NOT NULL");
        assertThat(columnType(body, "ROWS_SOURCE"))
                .as("le righe del file: nullable, perche' su un file interrotto il totale non si sa")
                .isEqualTo("BIGINT");
        assertThat(columnDeclaration(body, "ROWS_PROCESSED"))
                .as("le righe elaborate")
                .startsWith("BIGINT");
    }

    /**
     * {@code ROWS_SOURCE} deve restare nullable, e non e' una sfumatura.
     *
     * <p>E' l'unica colonna che distingue "il file aveva quarantamila righe" da "la lettura si e'
     * fermata a quarantamila": su un file interrotto il totale non lo conosce nessuno, e un
     * {@code NOT NULL} costringerebbe a scriverci il parziale — cioe' a far sembrare completato un
     * file che si e' rotto. Gli altri contatori sono invece sempre noti e partono da zero.</p>
     */
    @Test
    void soloRowsSourcePuoEssereNulloFraIContatori() {
        String body = fileTableBody();

        assertThat(columnType(body, "ROWS_SOURCE"))
                .as("un totale obbligatorio costringerebbe a scrivere il parziale di un file rotto")
                .doesNotContain("NOT NULL");
        for (String counter : List.of("ROWS_READ", "ROWS_PROCESSED", "ROWS_FAILED",
                "ROWS_TRUNCATED")) {
            assertThat(columnDeclaration(body, counter))
                    .as("%s deve avere un default e non essere nullo", counter)
                    .isEqualTo("BIGINT DEFAULT 0 NOT NULL");
        }
    }

    /**
     * Il progressivo non puo' essere un intero.
     *
     * <p>{@code FdrBlobNameParser} usa {@code BigInteger} perche' il cliente non ha posto un limite
     * al numero di cifre. Con {@code INTEGER} un file che la discovery accetta non sarebbe
     * registrabile qui, e il log fallirebbe proprio sul file piu' anomalo — quello per cui lo si
     * guarda.</p>
     */
    @Test
    void ilProgressivoNonPerdeCifre() {
        String type = columnType(fileTableBody(), "PROGRESSIVE");

        assertThat(type)
                .as("il parser usa BigInteger: un intero rifiuterebbe un progressivo valido")
                .isEqualTo("NUMERIC");
        assertThat(type).doesNotContain("INTEGER").doesNotContain("SMALLINT");
    }

    /** Il tentativo ha un default: la prima lettura di un file e' il tentativo uno, sempre. */
    @Test
    void ilTentativoHaUnDefault() {
        assertThat(columnDeclaration(fileTableBody(), "ATTEMPT_NO"))
                .isEqualTo("INTEGER DEFAULT 1 NOT NULL");
    }

    /**
     * Due FK, entrambe RESTRICT, e {@code STATUS_ID} nullable.
     *
     * <p>RESTRICT e non CASCADE per la stessa ragione delle altre operative: non esiste ancora una
     * retention FDR, e con CASCADE la prima pulizia scritta in futuro cancellerebbe in silenzio
     * l'audit dei file insieme al batch.</p>
     */
    @Test
    void ilLogPerFileHaDueForeignKeyEntrambeRestrict() {
        String ddl = stripComments(fileLog());

        assertThat(ddl).contains("CONSTRAINT FK_FDR_FILE_BATCH FOREIGN KEY (BATCH_ID)");
        assertThat(ddl).contains("CONSTRAINT FK_FDR_FILE_STATUS FOREIGN KEY (STATUS_ID)");
        assertThat(Pattern.compile("FOREIGN KEY").matcher(ddl).results().count()).isEqualTo(2);
        assertThat(countOf(ddl, "ON DELETE RESTRICT")).isEqualTo(2);
        assertThat(ddl).as("nessun CASCADE sull'audit").doesNotContain("ON DELETE CASCADE");
        assertThat(columnType(fileTableBody(), "STATUS_ID")).isEqualTo("BIGINT");
    }

    /**
     * Nessuna FK verso il dato business ne' verso lo staging.
     *
     * <p>Verso le business perche' un'anomalia del sorgente non deve poter essere bloccata da una
     * tabella di controllo. Verso {@code STG_INGEST_ERROR} perche' quella ha retention di sette
     * giorni e partizioni troncate: una FK bloccherebbe la pulizia oppure porterebbe via l'audit
     * con se'.</p>
     */
    @Test
    void ilLogPerFileNonReferenziaDatoBusinessNeStaging() {
        String ddl = stripComments(fileLog());

        assertThat(ddl).doesNotContain("REFERENCES ${schema_name}.FDR_FLUSSI_RENDICONTAZIONE");
        assertThat(ddl).doesNotContain("REFERENCES ${schema_name}.FDR_IUV_RENDICONTATI");
        assertThat(ddl).doesNotContain("STG_INGEST_ERROR");
    }

    /**
     * Nessuna UNIQUE, e in particolare non su (batch, blob).
     *
     * <p>Quel vincolo <strong>impedirebbe il retry</strong>: la ripresa di una giornata rilegge gli
     * stessi file nello stesso batch, quindi il secondo tentativo verrebbe rifiutato dal database.
     * I due tentativi si distinguono per {@code ATTEMPT_NO} e devono restare visibili entrambi —
     * "fallito al primo colpo, riuscito al secondo" sono due fatti, non uno.</p>
     */
    @Test
    void nessunaUniqueImpedisceLaSecondaLetturaDelloStessoFile() {
        String ddl = stripComments(fileLog()).replaceAll("\\s+", " ");

        assertThat(ddl).doesNotContain("UNIQUE");
        assertThat(ddl).doesNotContain("CREATE UNIQUE INDEX");
    }

    /** Nessun CHECK: gli stati del file li valida l'applicazione, come per le altre operative. */
    @Test
    void ilLogPerFileNonIrrigidisceGliStatiConUnCheck() {
        assertThat(stripComments(fileLog())).doesNotContain("CHECK (");
    }

    /**
     * Non partizionata, e non e' una dimenticanza.
     *
     * <p>Una giornata reale ha prodotto undici file; moltiplicando per i due dataset e per i
     * tentativi restano qualche migliaio di righe su tutto l'orizzonte. Partizionarla per simmetria
     * con le business aggiungerebbe 730 oggetti da mantenere senza alcun beneficio.</p>
     */
    @Test
    void ilLogPerFileNonEpartizionato() {
        String ddl = stripComments(fileLog());

        assertThat(ddl).doesNotContain("PARTITION BY");
        assertThat(ddl).doesNotContain("PARTITION OF");
    }

    /**
     * Tre indici, ciascuno per una query dichiarata.
     *
     * <p>Quello per giornata e dataset e' la domanda del cliente; quello per batch ricostruisce una
     * richiesta; quello su {@code STATUS_ID} serve la foreign key, che PostgreSQL non indicizza dal
     * lato figlio. Nessun indice su {@code BLOB_NAME}: non esiste una query che parta dal nome del
     * file, e aggiungerlo "per sicurezza" sarebbe il tipo di indice che nessuno rimuove piu'.</p>
     */
    @Test
    void ilLogPerFileHaSoloGliIndiciCheServonoAUnaQuery() {
        String ddl = stripComments(fileLog());

        assertThat(ddl).contains("IDX_FDR_FILE_DAY_DATASET");
        assertThat(ddl).contains("(BUSINESS_DAY, DATASET)");
        assertThat(ddl).contains("IDX_FDR_FILE_BATCH");
        assertThat(ddl).contains("IDX_FDR_FILE_STATUS");
        assertThat(indexNamesIn(ddl)).hasSize(3);
        assertThat(ddl).as("nessun indice su BLOB_NAME finche' non c'e' una query")
                .doesNotContain("(BLOB_NAME)");
    }

    /**
     * Il rollback cancella tabella e sequenza, in quest'ordine.
     *
     * <p>L'ordine inverso lascerebbe per un istante una tabella il cui default punta a una sequenza
     * che non c'e' piu'. E va detto che il rollback e' sicuro <em>solo</em> accettando di perdere
     * l'audit gia' registrato: non tocca dato business, perche' questa tabella non ne contiene.</p>
     */
    @Test
    void ilRollbackDelLogPerFileTogliePrimaLaTabellaPoiLaSequenza() {
        String body = fileLog();
        int at = body.indexOf("<rollback>");

        assertThat(at).as("il changeSet deve dichiarare un rollback").isGreaterThan(0);
        String rollback = body.substring(at);
        assertThat(rollback.indexOf("DROP TABLE"))
                .as("la tabella va cancellata prima della sequenza")
                .isLessThan(rollback.indexOf("DROP SEQUENCE"));
        assertThat(rollback).doesNotContain("FDR_FLUSSI_RENDICONTAZIONE")
                .doesNotContain("FDR_IUV_RENDICONTATI");
    }

    /**
     * Il changeSet deve dire a chiare lettere che non e' uno staging e non e' un checkpoint.
     *
     * <p>Sono le due letture sbagliate che la tabella invita a fare — porta conteggi per file e uno
     * stato COMPLETED per file — e la seconda e' pericolosa: usarla per saltare i file gia'
     * completati in un retry richiederebbe una garanzia di coerenza fra il commit del dato business
     * e quello di questa riga che non esiste.</p>
     */
    @Test
    void ilChangeSetDiceCheNonEunoStagingNeUnCheckpoint() {
        String comment = fileLog();

        assertThat(comment).containsIgnoringCase("NON E' UNO STAGING");
        assertThat(comment).containsIgnoringCase("checkpoint");
        assertThat(comment).as("la ragione per cui non c'e' la UNIQUE deve restare scritta")
                .containsIgnoringCase("impedirebbe il retry");
    }

    /** Il corpo della CREATE TABLE del log per file, senza commenti. */
    private static String fileTableBody() {
        String ddl = stripComments(fileLog());
        int start = ddl.indexOf("CREATE TABLE ${schema_name}.FDR_INGESTION_FILE (");
        assertThat(start).as("tabella del log per file non trovata").isGreaterThan(0);
        int end = ddl.indexOf("CONSTRAINT FDR_INGESTION_FILE_PK", start);
        assertThat(end).as("fine della CREATE TABLE non trovata").isGreaterThan(start);
        return ddl.substring(start, end);
    }

    // ─── Collisioni di naming sull'intero schema ─────────────────────────────────

    /**
     * Gli indici vivono nel namespace dello <strong>schema</strong>, non della tabella. Il modello
     * cliente lo ignora e dichiara {@code idx_id_flusso} e {@code idx_unique_id} su entrambe le
     * tabelle: applicato alla lettera, fallirebbe. Questo test guarda piu' in la' — verifica che
     * nessun nome FDR collida con un nome gia' usato da una qualunque migration dell'AS-IS, che e'
     * un errore che si scopre solo quando la migration gira.
     */
    @Test
    void iNomiDegliIndiciFdrNonCollidonoConNullaNelloSchema() {
        Set<String> fdrNames = indexNamesIn(fdrDdl());
        assertThat(fdrNames).as("nessun indice FDR trovato: il test non starebbe verificando nulla")
                .hasSizeGreaterThanOrEqualTo(12);

        Set<String> existing = new LinkedHashSet<>();
        try (Stream<Path> files = Files.walk(changelogDir())) {
            files.filter(p -> p.toString().endsWith(".xml"))
                    .filter(p -> !p.toString().replace('\\', '/').contains("/2.3.0.fdr/"))
                    .forEach(p -> {
                        try {
                            existing.addAll(indexNamesIn(Files.readString(p, StandardCharsets.UTF_8)));
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        assertThat(existing).as("nessun indice AS-IS letto: il confronto sarebbe vuoto")
                .isNotEmpty();

        List<String> collisions = fdrNames.stream().filter(existing::contains).toList();
        assertThat(collisions).as("nomi di indice gia' usati nello schema").isEmpty();
    }

    @Test
    void tuttiGliIdentificatoriFdrStannoSottoIlLimiteDiPostgres() {
        List<String> tooLong = new ArrayList<>();
        Matcher matcher = Pattern.compile(
                        "(?:CONSTRAINT|CREATE INDEX|sequenceName=\")\\s*([A-Za-z0-9_]{1,200})")
                .matcher(fdrDdl());
        while (matcher.find()) {
            if (matcher.group(1).length() > PG_IDENTIFIER_LIMIT) {
                tooLong.add(matcher.group(1));
            }
        }
        assertThat(tooLong).isEmpty();
    }

    // ─── Nessun DDL a runtime ────────────────────────────────────────────────────

    /**
     * La strategia approvata e' LIQUIBASE_PRECREATED_PARTITIONS_WITH_DEFAULT: le partizioni le crea
     * Liquibase, mai l'applicazione. Un {@code CREATE TABLE} emesso da Java sul percorso di
     * ingestion sarebbe DDL a runtime, con il suo lock e la sua imprevedibilita'. L'AS-IS non ne ha
     * nessuno; questo test fa in modo che FDR non sia il primo.
     *
     * <p>Cerca solo dentro i <strong>letterali di stringa</strong>, e non nel file intero: per
     * essere eseguito, il DDL deve stare fra virgolette. Cercando ovunque, il test segnalava una
     * frase in inglese - "no partition of its own" nel commento della guardia - cioe' falliva su
     * della documentazione corretta, che e' il modo piu' rapido per far disattivare un presidio
     * utile. I commenti vengono quindi esclusi esplicitamente.
     */
    @Test
    void nessunDdlARuntimeNelCodiceJava() {
        Path sources = projectRoot().resolve("src/main/java");
        assertThat(sources).as("sorgenti Java non trovati in %s", sources).exists();

        List<String> offenders = new ArrayList<>();
        try (Stream<Path> files = Files.walk(sources)) {
            files.filter(p -> p.toString().endsWith(".java")).forEach(p -> {
                try {
                    String body = Files.readString(p, StandardCharsets.UTF_8);
                    if (containsDdlLiteral(body)) {
                        offenders.add(p.getFileName().toString());
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        assertThat(offenders).as("DDL a runtime nel codice applicativo").isEmpty();
    }

    /** Una riga di codice, non di commento, che porta fra virgolette un CREATE TABLE o PARTITION OF. */
    private static boolean containsDdlLiteral(String body) {
        return body.lines().anyMatch(line -> {
            String trimmed = line.trim();
            if (trimmed.startsWith("*") || trimmed.startsWith("//") || trimmed.startsWith("/*")) {
                return false;
            }
            if (trimmed.indexOf('"') < 0) {
                return false;
            }
            String upper = trimmed.toUpperCase(Locale.ROOT);
            return upper.contains("CREATE TABLE") || upper.contains("PARTITION OF");
        });
    }

    // ─── Rollback ────────────────────────────────────────────────────────────────

    /**
     * Entrambi i changeSet dichiarano un rollback, e lo fanno dalla foglia alla radice: con le FK in
     * RESTRICT, cancellare BATCH prima di STATUS fallirebbe — ed e' il comportamento voluto.
     */
    @Test
    void entrambiIChangeSetHannoUnRollbackNellOrdineGiusto() {
        assertThat(business()).contains("<rollback>");
        assertThat(operational()).contains("<rollback>");

        String rollback = operational().substring(operational().indexOf("<rollback>"));
        assertThat(rollback.indexOf("FDR_INGESTION_EVENT"))
                .as("EVENT deve essere cancellata prima di STATUS")
                .isLessThan(rollback.indexOf("FDR_INGESTION_STATUS"));
        assertThat(rollback.indexOf("FDR_INGESTION_STATUS"))
                .as("STATUS deve essere cancellata prima di BATCH")
                .isLessThan(rollback.indexOf("FDR_INGESTION_BATCH"));
    }

    // ─── Utilita' ────────────────────────────────────────────────────────────────

    private static Set<String> indexNamesIn(String xml) {
        Set<String> names = new LinkedHashSet<>();
        Matcher sql = Pattern.compile("CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+(?:IF NOT EXISTS\\s+)?([A-Za-z0-9_]+)")
                .matcher(xml);
        while (sql.find()) {
            names.add(sql.group(1).toUpperCase(Locale.ROOT));
        }
        Matcher xmlTag = Pattern.compile("indexName=\"([A-Za-z0-9_]+)\"").matcher(xml);
        while (xmlTag.find()) {
            names.add(xmlTag.group(1).toUpperCase(Locale.ROOT));
        }
        return names;
    }

    /** Il corpo di una CREATE TABLE business, senza commenti. */
    private static String tableBody(String table) {
        String ddl = stripComments(business());
        int start = ddl.indexOf("CREATE TABLE ${schema_name}." + table + " (");
        assertThat(start).as("tabella non trovata: %s", table).isGreaterThan(0);
        int end = ddl.indexOf(") PARTITION BY", start);
        assertThat(end).as("fine della CREATE TABLE non trovata: %s", table).isGreaterThan(start);
        return ddl.substring(start, end);
    }

    /**
     * Il tipo che la colonna avra' davvero nel database: quello della {@code CREATE TABLE}, se
     * nessun changeSet successivo lo cambia, altrimenti quello dell'ultimo {@code ALTER COLUMN}.
     *
     * <p>Serve perche' una lunghezza puo' essere corretta solo con un changeSet nuovo, una volta
     * che il primo e' stato applicato su un ambiente. Un test che guardasse la sola
     * {@code CREATE TABLE} continuerebbe ad asserire il valore storico e direbbe il falso proprio
     * dopo una correzione — cioe' nel momento in cui serve di piu'.
     */
    private static String effectiveColumnType(String table, String column) {
        String type = columnType(tableBody(table), column);
        Pattern alter = Pattern.compile(
                "ALTER TABLE \\$\\{schema_name\\}\\." + table
                        + "\\s+ALTER COLUMN " + column + " TYPE ([A-Za-z0-9()]+)");

        for (String changelog : fdrChangelogs()) {
            String body = stripComments(changelog).replaceAll("\\s+", " ");
            // Il rollback dichiara l'ALTER inverso: va ignorato, altrimenti l'ultima occorrenza
            // trovata sarebbe sempre quella che annulla la modifica.
            int rollbackAt = body.indexOf("<rollback>");
            String forward = rollbackAt >= 0 ? body.substring(0, rollbackAt) : body;

            Matcher matcher = alter.matcher(forward);
            while (matcher.find()) {
                type = matcher.group(1);
            }
        }
        return type;
    }

    /** I nomi dei file changelog FDR presenti sul disco, in ordine. */
    private static List<String> fdrChangelogNames() {
        try (Stream<Path> files = Files.list(changelogDir().resolve("2.3.0.fdr"))) {
            return files.filter(p -> p.toString().endsWith(".xml"))
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Il contenuto di tutti i changelog FDR, in ordine di nome e quindi di applicazione. */
    private static List<String> fdrChangelogs() {
        try (Stream<Path> files = Files.list(changelogDir().resolve("2.3.0.fdr"))) {
            return files.filter(p -> p.toString().endsWith(".xml"))
                    .sorted()
                    .map(p -> {
                        try {
                            return Files.readString(p, StandardCharsets.UTF_8);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * La dichiarazione completa di una colonna, {@code DEFAULT} incluso e spazi normalizzati.
     *
     * <p>Distinta da {@link #columnType}, che il DEFAULT lo taglia via: sul log per file il default
     * dei contatori <em>e'</em> parte del requisito — una riga appena aperta deve dire zero, non
     * nulla.</p>
     */
    private static String columnDeclaration(String tableBody, String column) {
        Matcher matcher = Pattern.compile("^\\s*" + column + "\\s+([^,\\n]+)", Pattern.MULTILINE)
                .matcher(tableBody);
        assertThat(matcher.find()).as("colonna non trovata: %s", column).isTrue();
        return matcher.group(1).trim().replaceAll("\\s+", " ");
    }

    /** Il tipo dichiarato di una colonna, senza eventuale DEFAULT e senza virgola finale. */
    private static String columnType(String tableBody, String column) {
        Matcher matcher = Pattern.compile("^\\s*" + column + "\\s+([^,\\n]+)", Pattern.MULTILINE)
                .matcher(tableBody);
        assertThat(matcher.find()).as("colonna non trovata: %s", column).isTrue();
        return matcher.group(1).trim().split("\\s+DEFAULT\\s+")[0].trim();
    }

    private static int countOf(String haystack, String needle) {
        int count = 0;
        int at = haystack.indexOf(needle);
        while (at >= 0) {
            count++;
            at = haystack.indexOf(needle, at + needle.length());
        }
        return count;
    }

    // ─── 08: separazione operativa dal vecchio ingestor ──────────────────────────

    /**
     * Le due colonne che legavano l'audit FDR allo staging dell'ingestione ADX spariscono.
     *
     * <p>Verificato su DEV prima di scrivere il changeSet: {@code FDR_INGESTION_EVENT} aveva zero
     * righe, quindi zero valori da migrare o preservare. La condizione del requisito — "se sono
     * tutti NULL procedi col DROP" — era soddisfatta nel modo piu' forte possibile.
     */
    @Test
    void ilDecouplingTogliELeDueColonneDiStagingDaEvent() {
        String body = decoupling();

        assertThat(body)
                .as("il riferimento alla riga di staging non deve sopravvivere al decoupling")
                .contains("columnName=\"STG_ERROR_ID\"")
                .contains("columnName=\"STG_ERROR_CREATED_AT\"");
        assertThat(countOf(decouplingForward(), "<dropColumn"))
                .as("esattamente due DROP in avanti, uno per colonna. Il rollback ne ha altri tre,"
                        + " che tolgono token e lease: contarli insieme nasconderebbe un DROP di"
                        + " troppo fra quelli veri")
                .isEqualTo(2);
    }

    /**
     * Il possesso di una giornata: token, scadenza e processo, tutti e tre <strong>nullable</strong>.
     *
     * <p>La nullabilita' non e' pigrizia: una giornata in uno stato terminale <em>non ha</em> un
     * proprietario, e l'assenza del valore e' il modo corretto di dirlo. Un default — stringa vuota,
     * data lontana — costringerebbe ogni lettore a conoscere la convenzione, e il primo che non la
     * conoscesse leggerebbe "posseduta" su una giornata chiusa.
     */
    @Test
    void ilDecouplingAggiungeTokenLeaseEProcessoAStatus() {
        String body = decoupling();

        assertThat(body).contains("name=\"PROCESSING_TOKEN\" type=\"VARCHAR(100)\"");
        assertThat(body).contains("name=\"LEASE_EXPIRES_AT\" type=\"TIMESTAMP WITH TIME ZONE\"");
        assertThat(body).contains("name=\"PROCESSING_INSTANCE_ID\" type=\"VARCHAR(255)\"");
        assertThat(body)
                .as("nessuna delle tre puo' essere obbligatoria: una giornata conclusa non ha un "
                        + "proprietario, e lo dice l'assenza del valore")
                .doesNotContain("<constraints nullable=\"false\"/>");
    }

    /**
     * L'indice del recupero esiste, e non ne esiste uno sul token.
     *
     * <p>L'assenza e' la parte che vale la pena ancorare: non c'e' nessuna query che parta dal
     * token — compare solo come <em>condizione</em> di UPDATE su righe gia' individuate per
     * {@code (BATCH_ID, BUSINESS_DAY)} o per {@code ID} — quindi un indice su di esso verrebbe
     * scritto a ogni rinnovo del lease e letto mai.
     */
    @Test
    void ilDecouplingIndicizzaIlRecuperoENonIlToken() {
        String body = decoupling();

        assertThat(body).contains("indexName=\"IDX_FDR_STATUS_PROCESSING_LEASE\"");
        int index = body.indexOf("IDX_FDR_STATUS_PROCESSING_LEASE");
        String declaration = body.substring(index, body.indexOf("</createIndex>", index));
        assertThat(declaration)
                .as("STATUS in testa e LEASE_EXPIRES_AT a seguire: e' la forma della query del "
                        + "recupero")
                .contains("name=\"STATUS\"")
                .contains("name=\"LEASE_EXPIRES_AT\"");

        assertThat(countOf(body, "<createIndex"))
                .as("un solo indice nuovo: PROCESSING_TOKEN non ha una query che parta da lui")
                .isEqualTo(1);
    }

    /**
     * Il rollback esiste, e <strong>dichiara di non essere un ripristino</strong>.
     *
     * <p>È l'asserzione piu' importante delle quattro, e l'unica che riguarda la documentazione
     * invece della forma: un rollback che ricrea le colonne ma non i dati, e che non lo dica a
     * chiare lettere, verrebbe scambiato per reversibilita' proprio da chi lo sta per lanciare —
     * cioe' nel momento in cui scoprirlo e' troppo tardi.
     */
    @Test
    void ilDecouplingDichiaraCheIlRollbackNonEunRipristino() {
        assertThat(decoupling())
                .contains("<rollback>")
                .as("il rollback deve dire a chiare lettere che non riporta indietro i dati")
                .containsIgnoringCase("ROLLBACK STRUTTURALE != RIPRISTINO SEMANTICO COMPLETO");
    }

    /**
     * Nessuna nuova tabella e nessuna FK verso le strutture dell'ingestione ADX.
     *
     * <p>Il requisito e' esplicito su entrambi i fronti: gli errori di riga non devono produrre una
     * tabella nuova, e il modulo non deve riacquisire un legame fisico con lo staging che ha appena
     * lasciato.
     */
    @Test
    void ilDecouplingNonCreaTabelleNeLegamiVersoLAsIs() {
        // Senza togliere i commenti il test direbbe il falso: il changeSet PARLA diffusamente di
        // STG_INGEST_ERROR e INGEST_EXECUTION_LOG per spiegare perche' FDR se n'e' separato, e
        // cercarli nel testo grezzo troverebbe la spiegazione invece della cosa.
        String body = withoutXmlComments(decoupling());

        assertThat(body).doesNotContain("<createTable");
        assertThat(body).doesNotContain("<addForeignKeyConstraint");
        assertThat(body).doesNotContain("STG_INGEST_ERROR");
        assertThat(body).doesNotContain("INGEST_EXECUTION_LOG");
    }

    /** Il changeSet della separazione operativa. */
    private static String decoupling() {
        return read(DECOUPLING);
    }

    /**
     * Il solo percorso in avanti, senza il blocco di rollback.
     *
     * <p>Serve ai conteggi: il rollback usa le stesse istruzioni in senso opposto, quindi
     * contarle insieme renderebbe il numero inutile — e nasconderebbe proprio il caso che il
     * conteggio esiste per trovare, cioe' un DROP di troppo fra quelli veri.
     */
    private static String decouplingForward() {
        String body = withoutXmlComments(decoupling());
        int rollback = body.indexOf("<rollback>");
        return rollback < 0 ? body : body.substring(0, rollback);
    }

    private static String withoutXmlComments(String xml) {
        return xml.replaceAll("(?s)<!--.*?-->", "");
    }

    // ─── 09: INGESTED_AT sulle due business ──────────────────────────────────────

    private static String ingestedAt() {
        return read(INGESTED_AT);
    }

    /** Il master deve includerlo, altrimenti il changeSet esiste e non viene mai applicato. */
    @Test
    void ilMasterIncludeLAggiuntaDiIngestedAt() {
        assertThat(read(MASTER)).contains(INGESTED_AT);
    }

    /**
     * La colonna sta su <strong>entrambe</strong> le tabelle business.
     *
     * <p>Metterla su una sola renderebbe impossibile la cosa per cui esiste: riconoscere una
     * giornata scritta due volte. Le due tabelle si ingeriscono insieme, e una duplicazione le
     * tocca insieme.
     */
    @Test
    void ingestedAtEsisteSuEntrambeLeTabelleBusiness() {
        String body = withoutXmlComments(ingestedAt());

        assertThat(body).contains("tableName=\"FDR_FLUSSI_RENDICONTAZIONE\"");
        assertThat(body).contains("tableName=\"FDR_IUV_RENDICONTATI\"");
        assertThat(countOf(body, "name=\"INGESTED_AT\"")).isEqualTo(2);
    }

    /**
     * È l'unica colonna di quelle tabelle che porta il <strong>fuso</strong>, ed è corretto.
     *
     * <p>La regola FDR sui timestamp business — preservare l'orario scritto nel CSV senza applicare
     * offset, quindi {@code TIMESTAMP WITHOUT TIME ZONE} — vale per il dato del cliente. Questa non
     * è dato del cliente: è il nostro orologio, come tutte le colonne temporali delle tabelle
     * operative FDR.
     *
     * <p>Il test esiste perché l'errore naturale è l'opposto: qualcuno che applica la regola alla
     * lettera la porterebbe a {@code WITHOUT TIME ZONE} per coerenza con le colonne vicine, e
     * perderebbe l'informazione di fuso proprio sull'unico istante che è nostro.
     */
    @Test
    void ingestedAtPortaIlFusoPerchéNonVieneDalSorgente() {
        String body = withoutXmlComments(ingestedAt());

        assertThat(body)
                .as("e' il nostro orologio, non quello del file: TIMESTAMPTZ come le operative")
                .contains("name=\"INGESTED_AT\" type=\"TIMESTAMP WITH TIME ZONE\"");
    }

    /**
     * Obbligatoria e con default, perché le tabelle sono vuote ovunque.
     *
     * <p>Verificato prima di scrivere il changeSet: dev è stato azzerato il 27/11 e la catena 2.3.0
     * non è mai stata applicata su uat e prod. Senza righe preesistenti il {@code DEFAULT} non ha
     * nulla su cui scrivere un'ora falsa — che è invece ciò che accadrebbe su una tabella piena,
     * dove PostgreSQL valuta il default una volta sola all'{@code ALTER} e lo applica come costante.
     *
     * <p>Su una tabella popolata la scelta corretta tornerebbe a essere nullable: {@code NULL}
     * significa onestamente "non lo sappiamo", una data inventata no.
     */
    @Test
    void ingestedAtEobbligatoriaConDefaultPerchéLeTabelleSonoVuote() {
        String body = withoutXmlComments(ingestedAt());

        assertThat(body).contains("defaultValueComputed=\"now()\"");
        assertThat(countOf(body, "<constraints nullable=\"false\"/>"))
                .as("obbligatoria su entrambe: nessuna riga potra' nascere senza")
                .isEqualTo(2);
    }

    /** Il rollback toglie la colonna da entrambe, altrimenti ne lascerebbe una orfana. */
    @Test
    void ilRollbackDiIngestedAtTogliEntrambeLeColonne() {
        String body = withoutXmlComments(ingestedAt());
        int rollback = body.indexOf("<rollback>");

        assertThat(rollback).as("il changeSet deve essere reversibile").isGreaterThan(0);
        assertThat(countOf(body.substring(rollback), "<dropColumn")).isEqualTo(2);
    }

    /**
     * I changeSet precedenti non vengono toccati.
     *
     * <p>È la regola di questa catena, e vale anche qui: 01 e 04 sono già applicati negli ambienti,
     * quindi modificarli per aggiungere una colonna ne cambierebbe il checksum e la migration
     * fallirebbe. La colonna arriva con un changeSet nuovo.
     */
    @Test
    void laColonnaNonVieneAggiuntaModificandoIChangeSetGiaApplicati() {
        assertThat(read(BUSINESS))
                .as("il 01 e' gia' applicato: toccarlo ne cambierebbe il checksum")
                .doesNotContain("INGESTED_AT");
        assertThat(read("2.3.0.fdr/04_fdr_business_string_lengths.xml"))
                .doesNotContain("INGESTED_AT");
    }
}
