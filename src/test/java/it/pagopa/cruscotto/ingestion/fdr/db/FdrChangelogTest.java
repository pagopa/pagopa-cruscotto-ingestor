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

    /**
     * Tutto il DDL FDR, con i commenti XML rimossi.
     *
     * <p>Serve alle asserzioni di assenza: questi file <em>parlano</em> diffusamente di foreign key
     * e vincoli di unicita' per spiegare perche' non ci sono. Cercare "FOREIGN KEY" nel testo grezzo
     * troverebbe la spiegazione e non la cosa, e il test direbbe il falso.</p>
     */
    private static String fdrDdl() {
        return stripComments(business()) + "\n" + stripComments(operational());
    }

    private static String stripComments(String xml) {
        return xml.replaceAll("(?s)<!--.*?-->", " ");
    }

    // ─── Versioning: FDR appartiene alla catena 2.3.x ────────────────────────────

    @Test
    void ilMasterDiRilascioIncludeEntrambiIChangelogFdr() {
        String master = read(MASTER);

        assertThat(master).contains("2.3.0.fdr/01_fdr_business_schema.xml");
        assertThat(master).contains("2.3.0.fdr/02_fdr_operational_schema.xml");
        assertThat(master.indexOf("01_fdr_business_schema"))
                .as("lo schema business deve precedere quello operativo")
                .isLessThan(master.indexOf("02_fdr_operational_schema"));
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

    // ─── Le cinque tabelle ───────────────────────────────────────────────────────

    @Test
    void tutteECinqueLeTabelleSonoCreate() {
        String ddl = fdrDdl();

        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FLUSSI_RENDICONTAZIONE (");
        assertThat(ddl).contains("CREATE TABLE ${schema_name}.IUV_RENDICONTATI (");
        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FDR_INGESTION_BATCH (");
        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FDR_INGESTION_STATUS (");
        assertThat(ddl).contains("CREATE TABLE ${schema_name}.FDR_INGESTION_EVENT (");
    }

    @Test
    void unaSequenzaPerTabellaNessunaCondivisa() {
        String ddl = fdrDdl();
        List<String> expected = List.of(
                "SQ_FLUSSI_RENDICONTAZIONE", "SQ_IUV_RENDICONTATI",
                "SQ_FDR_INGESTION_BATCH", "SQ_FDR_INGESTION_STATUS", "SQ_FDR_INGESTION_EVENT");

        expected.forEach(sequence ->
                assertThat(ddl).contains("sequenceName=\"" + sequence + "\""));

        long declared = Pattern.compile("<createSequence").matcher(ddl).results().count();
        assertThat(declared)
                .as("una sequenza per tabella: ne' meno (condivisa) ne' di piu'")
                .isEqualTo(expected.size());
    }

    /**
     * Le tabelle business hanno {@code ID} come {@code BIGINT} e non {@code INTEGER} come il modello
     * cliente e come le tabelle ADX dell'AS-IS. Non e' uno stile: IUV_RENDICONTATI prende una riga
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
                "CONSTRAINT FLUSSI_RENDICONTAZIONE_PK PRIMARY KEY (DATE_INSERTED, ID)");
        assertThat(ddl).contains(
                "CONSTRAINT IUV_RENDICONTATI_PK PRIMARY KEY (DATE_INSERTED, ID)");
    }

    @Test
    void entrambeLeTabelleBusinessHannoLaPartizioneDefault() {
        String ddl = business();

        assertThat(ddl).contains(
                "CREATE TABLE ${schema_name}.FLUSSI_RENDICONTAZIONE_DEFAULT");
        assertThat(ddl).contains(
                "CREATE TABLE ${schema_name}.IUV_RENDICONTATI_DEFAULT");
        assertThat(countOf(ddl, "PARTITION OF ${schema_name}.FLUSSI_RENDICONTAZIONE DEFAULT"))
                .isEqualTo(1);
        assertThat(countOf(ddl, "PARTITION OF ${schema_name}.IUV_RENDICONTATI DEFAULT"))
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
    @Test
    void iNomiGeneratiDellePartizioniStannoSottoIlLimiteDiPostgres() {
        for (String prefix : List.of("FLUSSI_RENDICONTAZIONE_", "IUV_RENDICONTATI_")) {
            String partition = prefix + "20260101";
            assertThat(partition.length() + "_pkey".length())
                    .as("nome di partizione troppo lungo: %s", partition)
                    .isLessThanOrEqualTo(PG_IDENTIFIER_LIMIT);
        }
    }

    // ─── Garanzie negative: cio' che il modello cliente aveva e che e' stato tolto ─

    /**
     * Il modello cliente dichiara {@code ALTER TABLE "IUV_RENDICONTATI" ADD CONSTRAINT ...
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

        assertThat(ddl).contains("IDX_FLUSSI_REND_UNIQUE_ID");
        assertThat(ddl).contains("IDX_IUV_REND_UNIQUE_ID");
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
        for (String table : List.of("FLUSSI_RENDICONTAZIONE", "IUV_RENDICONTATI")) {
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
     * Le lunghezze sono quelle del modello cliente ovunque i dati reali non le smentiscano.
     * {@code ID_FLUSSO = 50} e' una <strong>decisione del cliente</strong>, non una nostra scelta:
     * sostituisce sia la sua proposta originale di 30, che i sample smentivano (osservati 35), sia
     * la baseline provvisoria di 64 adottata in attesa della risposta. {@code UNIQUE_ID} resta a 64
     * ed e' ancora provvisorio — la conferma riguardava solo ID_FLUSSO.
     */
    @Test
    void leLunghezzeDiFlussiRendicontazioneSonoQuelleConcordate() {
        String table = tableBody("FLUSSI_RENDICONTAZIONE");

        assertThat(columnType(table, "ID_FLUSSO")).isEqualTo("VARCHAR(50)");
        assertThat(columnType(table, "CAUSALE")).isEqualTo("VARCHAR(30)");
        assertThat(columnType(table, "ID_DOMINIO")).isEqualTo("VARCHAR(11)");
        assertThat(columnType(table, "PSP")).isEqualTo("VARCHAR(15)");
        assertThat(columnType(table, "INT_PSP")).isEqualTo("VARCHAR(11)");
        assertThat(columnType(table, "UNIQUE_ID")).isEqualTo("VARCHAR(64)");
    }

    @Test
    void leLunghezzeDiIuvRendicontatiSonoQuelleConcordate() {
        String table = tableBody("IUV_RENDICONTATI");

        assertThat(columnType(table, "IUV")).isEqualTo("VARCHAR(35)");
        assertThat(columnType(table, "IUR")).isEqualTo("VARCHAR(35)");
        assertThat(columnType(table, "ID_FLUSSO")).isEqualTo("VARCHAR(50)");
        assertThat(columnType(table, "ID_DOMINIO")).isEqualTo("VARCHAR(11)");
        assertThat(columnType(table, "PSP")).isEqualTo("VARCHAR(15)");
        assertThat(columnType(table, "INT_PSP")).isEqualTo("VARCHAR(11)");
        assertThat(columnType(table, "UNIQUE_ID")).isEqualTo("VARCHAR(64)");
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
        for (String table : List.of("FLUSSI_RENDICONTAZIONE", "IUV_RENDICONTATI")) {
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
        String flussi = tableBody("FLUSSI_RENDICONTAZIONE");
        String iuv = tableBody("IUV_RENDICONTATI");

        for (String column : List.of("ID_FLUSSO", "ID_DOMINIO", "PSP", "INT_PSP", "UNIQUE_ID",
                "DATA_ORA_FLUSSO", "INSERTED_TIMESTAMP")) {
            assertThat(columnType(iuv, column))
                    .as("tipo divergente per %s fra le due tabelle", column)
                    .isEqualTo(columnType(flussi, column));
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
     * Nessun clamp e nessun troncamento sulle colonne business: le lunghezze sono provvisorie, e un
     * valore accorciato in silenzio nasconderebbe esattamente l'informazione che serve a correggere
     * il contratto con il cliente. Deve fallire l'INSERT ed emergere.
     */
    @Test
    void leColonneBusinessNonAmmettonoTroncamento() {
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

        assertThat(ddl).doesNotContain("REFERENCES ${schema_name}.FLUSSI_RENDICONTAZIONE");
        assertThat(ddl).doesNotContain("REFERENCES ${schema_name}.IUV_RENDICONTATI");
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
     */
    @Test
    void nessunDdlARuntimeNelCodiceJava() {
        Path sources = projectRoot().resolve("src/main/java");
        assertThat(sources).as("sorgenti Java non trovati in %s", sources).exists();

        List<String> offenders = new ArrayList<>();
        try (Stream<Path> files = Files.walk(sources)) {
            files.filter(p -> p.toString().endsWith(".java")).forEach(p -> {
                try {
                    String body = Files.readString(p, StandardCharsets.UTF_8).toUpperCase(Locale.ROOT);
                    if (body.contains("CREATE TABLE") || body.contains("PARTITION OF")) {
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
}
