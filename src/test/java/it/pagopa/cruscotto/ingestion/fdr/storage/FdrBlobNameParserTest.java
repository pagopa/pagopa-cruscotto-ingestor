package it.pagopa.cruscotto.ingestion.fdr.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigInteger;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Le regole del nome file FDR, una per una.
 *
 * <p>Qui sta quasi tutto il rischio di questo step: il servizio non fa che elencare e delegare,
 * mentre ogni decisione — questo file e' nostro? e' del giorno giusto? che progressivo ha? — si
 * prende in questa classe, senza Azure di mezzo.
 *
 * <p>La maggior parte dei test verifica una <strong>classificazione</strong>, non un "no": e' la
 * differenza fra accorgersi che un flusso del 16 e' finito nella cartella del 15 — sintomo di un
 * problema a monte — e archiviarlo insieme ai documenti estranei.
 */
class FdrBlobNameParserTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    // ─── Prefisso giornaliero ────────────────────────────────────────────────────

    @ParameterizedTest
    @CsvSource({
            "2026-09-15, 20260915/",
            "2026-01-01, 20260101/",
            "2026-12-31, 20261231/",
            "2028-02-29, 20280229/"   // anno bisestile: nessun caso particolare, ma va provato
    })
    void ilPrefissoDerivaSoloDallaData(LocalDate day, String expected) {
        assertThat(FdrBlobNameParser.dayPrefix(day)).isEqualTo(expected);
    }

    /**
     * Il prefisso parte da una {@link LocalDate}, quindi nessun fuso partecipa alla decisione. E' la
     * ragione per cui il tipo in ingresso non e' un {@code Instant}: su un pod con TimeZone diversa
     * lo stesso istante indicherebbe due cartelle diverse.
     */
    @Test
    void ilPrefissoNonDipendeDalFusoOrarioDellaMacchina() {
        String before = FdrBlobNameParser.dayPrefix(DAY);
        TimeZoneSwap.runWith("Pacific/Kiritimati", () ->
                assertThat(FdrBlobNameParser.dayPrefix(DAY)).isEqualTo(before));
        TimeZoneSwap.runWith("Pacific/Midway", () ->
                assertThat(FdrBlobNameParser.dayPrefix(DAY)).isEqualTo(before));
    }

    // ─── Nomi validi ─────────────────────────────────────────────────────────────

    @ParameterizedTest
    @CsvSource({
            "FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz,  FLUSSI_RENDICONTAZIONE, 1",
            "FLUSSI_RENDICONTAZIONE_20260915-2.csv.gz,  FLUSSI_RENDICONTAZIONE, 2",
            "FLUSSI_RENDICONTAZIONE_20260915-10.csv.gz, FLUSSI_RENDICONTAZIONE, 10",
            "IUV_RENDICONTATI_20260915-1.csv.gz,        IUV_RENDICONTATI, 1",
            "IUV_RENDICONTATI_20260915-3.csv.gz,        IUV_RENDICONTATI, 3",
            "IUV_RENDICONTATI_20260915-10.csv.gz,       IUV_RENDICONTATI, 10",
            "IUV_RENDICONTATI_20260915-001.csv.gz,      IUV_RENDICONTATI, 1",
            "IUV_RENDICONTATI_20260915-00010.csv.gz,    IUV_RENDICONTATI, 10",
            "IUV_RENDICONTATI_20260915-999999999.csv.gz, IUV_RENDICONTATI, 999999999"
    })
    void iNomiValidiVengonoScomposti(String fileName, FdrDataset dataset, String progressive) {
        FdrBlobNameParser.Outcome outcome = FdrBlobNameParser.parse("20260915/" + fileName, DAY);

        assertThat(outcome.isValid()).as("atteso valido: %s", fileName).isTrue();
        assertThat(outcome.dataset()).isEqualTo(dataset);
        assertThat(outcome.businessDay()).isEqualTo(DAY);
        assertThat(outcome.progressive()).isEqualTo(new BigInteger(progressive));
        assertThat(outcome.blobName()).isEqualTo("20260915/" + fileName);
    }

    /**
     * Gli zeri iniziali sono ammessi. Il cliente ha definito il naming ma non il numero di cifre,
     * quindi rifiutare {@code -001} sarebbe una regola NOSTRA imposta ai suoi file, e farebbe
     * l'unica cosa che la discovery non deve mai fare: perdere un blob che esiste.
     *
     * <p>Il valore numerico coincide, ma i due file restano distinti: a dirlo e' il nome.
     */
    @Test
    void gliZeriInizialiNonCambianoIlValoreMaNonRendonoIlFileInvalido() {
        FdrBlobNameParser.Outcome uno = parse("IUV_RENDICONTATI_20260915-1.csv.gz");
        FdrBlobNameParser.Outcome zeroZeroUno = parse("IUV_RENDICONTATI_20260915-001.csv.gz");

        assertThat(uno.isValid()).isTrue();
        assertThat(zeroZeroUno.isValid()).isTrue();
        assertThat(zeroZeroUno.progressive()).isEqualTo(uno.progressive());
        assertThat(zeroZeroUno.blobName()).isNotEqualTo(uno.blobName());
    }

    /**
     * Nessun tetto al numero di cifre, quindi nessun overflow possibile: un nome formalmente valido
     * non deve mai trasformarsi in un errore tecnico dell'intera giornata perche' il valore non
     * entrava in un tipo Java. E' la ragione per cui il progressivo e' un {@link BigInteger} e non
     * un {@code int} o un {@code long}.
     */
    @Test
    void unProgressivoOltreLaCapacitaDiLongRestaValidoEordinabile() {
        String enorme = "9223372036854775808";   // Long.MAX_VALUE + 1
        FdrBlobNameParser.Outcome outcome = parse("IUV_RENDICONTATI_20260915-" + enorme + ".csv.gz");

        assertThat(outcome.isValid()).isTrue();
        assertThat(outcome.progressive()).isEqualTo(new BigInteger(enorme));
        assertThat(outcome.progressive()).isGreaterThan(BigInteger.valueOf(Long.MAX_VALUE));
    }

    /**
     * Un nome assente non e' un'anomalia del dato da classificare: e' un difetto di chi chiama, e
     * deve dirlo. Il fallimento immediato con il nome del parametro vale molto piu' di una
     * {@code NullPointerException} nuda sollevata dentro un ciclo su migliaia di blob, dove non si
     * capirebbe nemmeno quale campo mancava.
     */
    @Test
    void unNomeAssenteFallisceSubitoEdicePerche() {
        // hasMessage e non hasMessageContaining, ed e' la differenza fra un test che verifica e uno
        // che si limita a passare: senza la guardia esplicita Java solleverebbe comunque una
        // NullPointerException, e dal 14 il suo messaggio contiene gia' il nome della variabile
        // ("Cannot invoke ... because \"blobName\" is null"). Con il contains, il test sarebbe
        // verde in entrambi i casi e non proteggerebbe nulla.
        assertThatThrownBy(() -> FdrBlobNameParser.parse(null, DAY))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("blobName");
    }

    /** Il nome puo' arrivare senza cartella: il parser non deve dipendere dal prefisso. */
    @Test
    void ilNomeFunzionaAncheSenzaLaCartellaDavanti() {
        assertThat(FdrBlobNameParser.parse("IUV_RENDICONTATI_20260915-1.csv.gz", DAY).isValid()).isTrue();
    }

    // ─── Classificazione delle anomalie ──────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = {
            "ALTRO_20260915-1.csv.gz",
            "readme.txt",
            "FLUSSI_20260915-1.csv.gz",   // prefisso troncato: non e' nessuno dei due dataset
            "RENDICONTAZIONE_20260915-1.csv.gz"
    })
    void datasetSconosciuto(String fileName) {
        assertThat(reasonFor(fileName)).isEqualTo(FdrInvalidEntry.Reason.UNKNOWN_DATASET);
    }

    /**
     * Un nome che si annuncia come FDR ma non rispetta il contratto e' un problema diverso da un
     * documento che non e' mai stato nostro, e va segnalato come tale: qui il dataset si riconosce,
     * quindi la ragione e' "malformato" e non "sconosciuto". E' la distinzione che dice a chi legge
     * se guardare al produttore del file o a chi ha sbagliato cartella.
     */
    @Test
    void unNomeCheIniziaComeFdrMaNonRispettaIlContrattoEmalformato() {
        assertThat(reasonFor("IUV_RENDICONTATI_EXTRA_20260915-1.csv.gz"))
                .isEqualTo(FdrInvalidEntry.Reason.MALFORMED_NAME);
    }

    /**
     * L'estensione sbagliata merita una ragione propria, e il caso {@code .zip} in particolare: la
     * documentazione FDR iniziale parlava di ZIP, quindi un file che arrivasse davvero cosi' non
     * sarebbe un refuso ma un cambio di contratto, da far emergere come tale.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "IUV_RENDICONTATI_20260915-1.csv",
            "IUV_RENDICONTATI_20260915-1.zip",
            "IUV_RENDICONTATI_20260915-1.gz",
            "IUV_RENDICONTATI_20260915-1.csv.gz.tmp",
            "FLUSSI_RENDICONTAZIONE_20260915-1"
    })
    void estensioneSbagliata(String fileName) {
        assertThat(reasonFor(fileName)).isEqualTo(FdrInvalidEntry.Reason.WRONG_EXTENSION);
    }

    /**
     * Il caso che non va mai risolto in silenzio. Un file del 16 nella cartella del 15 e' ben
     * formato: attribuirlo al 15 scriverebbe dati nella partizione sbagliata, e nessuno se ne
     * accorgerebbe.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "IUV_RENDICONTATI_20260916-1.csv.gz",
            "IUV_RENDICONTATI_20260914-1.csv.gz",
            "FLUSSI_RENDICONTAZIONE_20250915-1.csv.gz"
    })
    void giornoSbagliato(String fileName) {
        assertThat(reasonFor(fileName)).isEqualTo(FdrInvalidEntry.Reason.WRONG_DAY);
    }

    /**
     * Progressivo non accettabile: tutto cio' che non e' una sequenza di cifre di valore
     * strettamente positivo. Lo zero non e' una posizione in una sequenza, in nessuna delle sue
     * scritture; il resto semplicemente non e' un numero.
     *
     * <p>Non sono in elenco gli zeri iniziali e i valori molto grandi: sono validi, e hanno i
     * propri test.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "IUV_RENDICONTATI_20260915-0.csv.gz",
            "IUV_RENDICONTATI_20260915-000.csv.gz",
            "IUV_RENDICONTATI_20260915--1.csv.gz",
            "IUV_RENDICONTATI_20260915-X.csv.gz",
            "IUV_RENDICONTATI_20260915-1a.csv.gz",
            "IUV_RENDICONTATI_20260915-a1.csv.gz",
            "IUV_RENDICONTATI_20260915-1.5.csv.gz",
            "IUV_RENDICONTATI_20260915-1 .csv.gz",
            "IUV_RENDICONTATI_20260915-+1.csv.gz"
    })
    void progressivoNonValido(String fileName) {
        assertThat(reasonFor(fileName)).isEqualTo(FdrInvalidEntry.Reason.INVALID_PROGRESSIVE);
    }

    /** Nessun nome, per quanto assurdo, deve propagare un'eccezione: viene classificato. */
    @ParameterizedTest
    @ValueSource(strings = {
            "IUV_RENDICONTATI_20260915-99999999999999999999999999999999999999999999.csv.gz",
            "IUV_RENDICONTATI_20260915-X.csv.gz",
            "IUV_RENDICONTATI_20260915-000.csv.gz"
    })
    void nessunNomeFaEsplodereIlParsing(String fileName) {
        assertThat(FdrBlobNameParser.parse("20260915/" + fileName, DAY)).isNotNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "IUV_RENDICONTATI_2026091-1.csv.gz",      // sette cifre
            "IUV_RENDICONTATI_202609150-1.csv.gz",    // nove cifre
            "IUV_RENDICONTATI_20261345-1.csv.gz",     // otto cifre che non sono una data
            "IUV_RENDICONTATI_20260915.csv.gz",       // manca il progressivo
            "IUV_RENDICONTATI_-1.csv.gz",             // manca la data
            "IUV_RENDICONTATI_20260915-.csv.gz"       // dash senza progressivo
    })
    void nomeMalformato(String fileName) {
        assertThat(reasonFor(fileName)).isEqualTo(FdrInvalidEntry.Reason.MALFORMED_NAME);
    }

    /**
     * Il listing piatto di Azure restituisce tutto cio' che sta sotto il prefisso, sottocartelle
     * comprese. Appiattire un percorso annidato significherebbe elaborare un file da una struttura
     * che nessuno ha concordato.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "20260915/archivio/IUV_RENDICONTATI_20260915-1.csv.gz",
            "20260915/tmp/sub/FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz"
    })
    void percorsoAnnidato(String blobName) {
        assertThat(FdrBlobNameParser.parse(blobName, DAY).invalidReason())
                .isEqualTo(FdrInvalidEntry.Reason.NESTED_PATH);
    }

    /**
     * L'ordine dei controlli e' una scelta diagnostica: un file che non e' nostro viene segnalato
     * come tale anche se ha pure l'estensione sbagliata. Lamentarsi dell'estensione di un documento
     * altrui manderebbe chi legge nella direzione sbagliata.
     */
    @Test
    void unFileEstraneoNonVieneGiudicatoSullEstensione() {
        assertThat(reasonFor("ALTRO_20260915-1.zip")).isEqualTo(FdrInvalidEntry.Reason.UNKNOWN_DATASET);
    }

    /** La classificazione porta sempre con se' il nome, altrimenti l'anomalia non e' ritrovabile. */
    @Test
    void laClassificazioneConservaIlNomeDelBlob() {
        FdrInvalidEntry entry = FdrBlobNameParser
                .parse("20260915/ALTRO_20260915-1.csv.gz", DAY).toInvalidEntry();

        assertThat(entry.blobName()).isEqualTo("20260915/ALTRO_20260915-1.csv.gz");
        assertThat(entry.reason()).isEqualTo(FdrInvalidEntry.Reason.UNKNOWN_DATASET);
    }

    private static FdrBlobNameParser.Outcome parse(String fileName) {
        return FdrBlobNameParser.parse("20260915/" + fileName, DAY);
    }

    private static FdrInvalidEntry.Reason reasonFor(String fileName) {
        return FdrBlobNameParser.parse("20260915/" + fileName, DAY).invalidReason();
    }

    /** Esegue un blocco con il fuso di default cambiato, ripristinandolo sempre. */
    private static final class TimeZoneSwap {
        static void runWith(String zoneId, Runnable body) {
            java.util.TimeZone original = java.util.TimeZone.getDefault();
            try {
                java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zoneId));
                body.run();
            } finally {
                java.util.TimeZone.setDefault(original);
            }
        }
    }
}
