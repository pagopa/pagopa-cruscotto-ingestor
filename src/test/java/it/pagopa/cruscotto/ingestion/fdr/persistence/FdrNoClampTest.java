package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrFlussoRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrIuvRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Nessuna colonna oltre CAUSALE viene accorciata, mai.
 *
 * <p>E' la garanzia che tiene in piedi la fedelta' al sorgente, ed e' facile da perdere per
 * distrazione: basta che qualcuno, vedendo fallire un INSERT per un valore troppo lungo, applichi
 * a quella colonna lo stesso clamp che su CAUSALE e' autorizzato. Qui i valori partono gia' fuori
 * misura e devono arrivare al binding <strong>interi</strong>, cosi' che sia il database a
 * rifiutarli e il difetto emerga.
 *
 * <p>Non e' zelo: un UNIQUE_ID accorciato non lega piu' gli IUV alla loro revisione, uno IUV
 * accorciato non si trova piu' con una ricerca, un INT_PSP accorciato non identifica piu'
 * l'intermediario. Accorciare qui non salva una riga, ne produce una sbagliata.
 */
class FdrNoClampTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    /** ID_FLUSSO e' VARCHAR(50): 60 caratteri devono arrivare interi al binding. */
    @Test
    void idFlussoOltreIlLimiteNonVieneAccorciato() throws SQLException {
        String lungo = "F".repeat(60);

        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE, flussoCon(lungo,
                "50ea4624-ccbc-462a-96f3-5b2726f81ae3", "14847241008"));

        assertThat(bound.get(3)).isEqualTo(lungo);
    }

    /** UNIQUE_ID e' VARCHAR(36), la lunghezza esatta di un UUID: niente margine, niente taglio. */
    @Test
    void uniqueIdOltreIlLimiteNonVieneAccorciato() throws SQLException {
        String lungo = "u".repeat(40);

        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE,
                flussoCon("F1", lungo, "14847241008"));

        assertThat(bound.get(12)).isEqualTo(lungo);
    }

    /** INT_PSP e' VARCHAR(35): il valore estero osservato ne occupa 14, ma 40 non si tronca. */
    @Test
    void intPspOltreIlLimiteNonVieneAccorciato() throws SQLException {
        String lungo = "NL853935051B01-" + "x".repeat(25);

        Map<Integer, Object> boundFlusso = BoundValues.of(FdrFlussoInsert.INSTANCE,
                flussoCon("F1", "u-1", lungo));
        Map<Integer, Object> boundIuv = BoundValues.of(FdrIuvInsert.INSTANCE, iuvCon("00001",
                "IUR", lungo));

        assertThat(boundFlusso.get(11)).isEqualTo(lungo);
        assertThat(boundIuv.get(12)).isEqualTo(lungo);
    }

    /** IUV e IUR sono VARCHAR(35) e restano interi, zeri iniziali compresi. */
    @Test
    void iuvEIurOltreIlLimiteNonVengonoAccorciati() throws SQLException {
        String iuvLungo = "0".repeat(30) + "1234567890";
        String iurLungo = "R".repeat(40);

        Map<Integer, Object> bound = BoundValues.of(FdrIuvInsert.INSTANCE,
                iuvCon(iuvLungo, iurLungo, "INT"));

        assertThat(bound.get(2)).isEqualTo(iuvLungo);
        assertThat(bound.get(3)).isEqualTo(iurLungo);
    }

    /** ID_DOMINIO e' VARCHAR(11) ed e' il limite piu' stretto dello schema: nemmeno quello si tocca. */
    @Test
    void idDominioEPspOltreIlLimiteNonVengonoAccorciati() throws SQLException {
        FdrFlussoRow row = new FdrFlussoRow(DAY, null, "F1", null, null, "causale", null, null,
                "8000107067300000", "PSP-MOLTO-LUNGO-OLTRE-QUINDICI", "INT", "u-1", null);

        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE, row);

        assertThat(bound.get(9)).isEqualTo("8000107067300000");
        assertThat(bound.get(10)).isEqualTo("PSP-MOLTO-LUNGO-OLTRE-QUINDICI");
    }

    /**
     * ALL_DATES va su TEXT, quindi non ha un limite da rispettare: la verifica e' che non venga
     * comunque abbreviato per prudenza, perche' la sua semantica non e' certificata e un valore
     * tagliato sarebbe indistinguibile da uno breve.
     */
    @Test
    void allDatesLungoNonVieneAbbreviato() throws SQLException {
        String lungo = "[" + "\"2026-09-14T00:00:00Z\",".repeat(200) + "\"2026-09-15T00:00:00Z\"]";

        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE,
                new FdrFlussoRow(DAY, null, "F1", null, null, "c", null, null, null, null, null,
                        "u-1", lungo));

        assertThat(bound.get(13)).isEqualTo(lungo);
    }

    private static FdrFlussoRow flussoCon(String idFlusso, String uniqueId, String intPsp) {
        return new FdrFlussoRow(DAY, LocalDateTime.parse("2026-09-15T08:38:01"), idFlusso,
                null, null, "causale", 1, new BigDecimal("34.51"), "80001070673", "IFSPIT21",
                intPsp, uniqueId, "[]");
    }

    private static FdrIuvRow iuvCon(String iuv, String iur, String intPsp) {
        return new FdrIuvRow(DAY, iuv, iur, new BigDecimal("131"), 0, null, 1, "F1", null,
                "00242200517", "ABI08811", intPsp, "u-1", null);
    }
}
