package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Dalla riga di CSV alla riga di business, sulle righe reali dei due dataset.
 *
 * <p>Oltre alla mappatura campo per campo, qui stanno due garanzie che valgono piu' delle altre: la
 * giornata viene dalla cartella e non dal file, e la causale <strong>non</strong> viene accorciata
 * in lettura.
 */
class FdrRowMapperTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    private static final List<String> FLUSSI_HEADER = List.of(
            "ID_FLUSSO", "DATA_ORA_FLUSSO", "INSERTED_TIMESTAMP", "DATA_REGOLAMENTO", "CAUSALE",
            "NUM_PAGAMENTI", "SOMMA_VERSATA", "ID_DOMINIO", "PSP", "INT_PSP", "UNIQUE_ID",
            "ALL_DATES");

    private static final List<String> IUV_HEADER = List.of(
            "IUV", "IUR", "IMPORTO", "COD_ESITO", "DATA_ESITO_SINGOLO_PAGAMENTO", "IDSP",
            "ID_FLUSSO", "DATA_ORA_FLUSSO", "ID_DOMINIO", "PSP", "INT_PSP", "UNIQUE_ID",
            "INSERTED_TIMESTAMP");

    private final FdrCsvHeader flussiHeader =
            FdrCsvHeader.of(FLUSSI_HEADER, FdrDataset.FLUSSI_RENDICONTAZIONE);
    private final FdrCsvHeader iuvHeader =
            FdrCsvHeader.of(IUV_HEADER, FdrDataset.IUV_RENDICONTATI);

    // ─── FLUSSI ──────────────────────────────────────────────────────────────────

    @Test
    void mappaLaRigaRealeDiFlussi() {
        FdrFlussoRow row = FdrFlussoRowMapper.INSTANCE.map(flussiHeader, flussiFields(), DAY);

        assertThat(row.dataset()).isEqualTo(FdrDataset.FLUSSI_RENDICONTAZIONE);
        assertThat(row.dateInserted()).isEqualTo(DAY);
        assertThat(row.idFlusso()).isEqualTo("2026-09-14IFSPIT21-CU20260000000WX4");
        assertThat(row.dataOraFlusso()).isEqualTo(Instant.parse("2026-09-14T22:00:00Z"));
        assertThat(row.insertedTimestamp()).isEqualTo(Instant.parse("2026-09-15T08:38:01.76163Z"));
        assertThat(row.dataRegolamento()).isEqualTo(Instant.parse("2026-09-14T00:00:00Z"));
        assertThat(row.causale()).isEqualTo("3604220120371326480320003200IT");
        assertThat(row.numPagamenti()).isEqualTo(1);
        assertThat(row.sommaVersata()).isEqualTo(new BigDecimal("34.51"));
        assertThat(row.idDominio()).isEqualTo("80001070673");
        assertThat(row.psp()).isEqualTo("IFSPIT21");
        assertThat(row.intPsp()).isEqualTo("14847241008");
        assertThat(row.uniqueId()).isEqualTo("50ea4624-ccbc-462a-96f3-5b2726f81ae3");
        assertThat(row.allDates()).isEqualTo("[\"2026-09-14T00:00:00.0000000Z\"]");
    }

    /**
     * La causale resta intera. Il cliente ha chiesto di tenere la colonna a 30 caratteri e di
     * accorciare il valore, ma l'accorciamento appartiene alla scrittura su database: qui serve
     * poter ancora confrontare cio' che e' stato scritto con cio' che il file conteneva. Nei dati
     * reali il 21,71% delle righe arriva a 35 caratteri, quindi questo caso e' la norma, non il
     * limite.
     */
    @Test
    void laCausaleNonVieneAccorciataInLettura() {
        List<String> fields = flussiFields();
        String lunga = "0123456789012345678901234567890123";
        fields.set(4, lunga);

        FdrFlussoRow row = FdrFlussoRowMapper.INSTANCE.map(flussiHeader, fields, DAY);

        assertThat(row.causale()).isEqualTo(lunga).hasSize(34);
    }

    /**
     * {@code ALL_DATES} resta il testo della sorgente: la sua semantica di business non e' ancora
     * certificata, e qualunque parsing sarebbe un'interpretazione nostra applicata in modo
     * distruttivo.
     */
    @Test
    void allDatesRestaTestoGrezzo() {
        List<String> fields = flussiFields();
        fields.set(11, "[\"2026-09-14T00:00:00Z\",\"2026-09-11T00:00:00Z\"]");

        assertThat(FdrFlussoRowMapper.INSTANCE.map(flussiHeader, fields, DAY).allDates())
                .isEqualTo("[\"2026-09-14T00:00:00Z\",\"2026-09-11T00:00:00Z\"]");
    }

    /**
     * La giornata e' quella della cartella del blob, non una data ricavata dal contenuto. Qui
     * {@code ID_FLUSSO} comincia con il 14 settembre mentre la cartella e' quella del 15: il valore
     * del campo non deve avere alcun effetto sulla partizione in cui la riga finira'.
     */
    @Test
    void laGiornataVieneDalParametroENonDaIdFlusso() {
        FdrFlussoRow row = FdrFlussoRowMapper.INSTANCE.map(flussiHeader, flussiFields(), DAY);

        assertThat(row.idFlusso()).startsWith("2026-09-14");
        assertThat(row.dateInserted()).isEqualTo(LocalDate.of(2026, 9, 15));
    }

    @Test
    void iCampiVuotiDiFlussiDiventanoNull() {
        List<String> fields = new ArrayList<>(List.of("", "", "", "", "", "", "", "", "", "", "", ""));

        FdrFlussoRow row = FdrFlussoRowMapper.INSTANCE.map(flussiHeader, fields, DAY);

        assertThat(row.idFlusso()).isNull();
        assertThat(row.causale()).isNull();
        assertThat(row.numPagamenti()).isNull();
        assertThat(row.sommaVersata()).isNull();
        assertThat(row.insertedTimestamp()).isNull();
        assertThat(row.dateInserted()).isEqualTo(DAY);
    }

    // ─── IUV ─────────────────────────────────────────────────────────────────────

    @Test
    void mappaLaRigaRealeDiIuv() {
        FdrIuvRow row = FdrIuvRowMapper.INSTANCE.map(iuvHeader, iuvFields(), DAY);

        assertThat(row.dataset()).isEqualTo(FdrDataset.IUV_RENDICONTATI);
        assertThat(row.dateInserted()).isEqualTo(DAY);
        assertThat(row.iuv()).isEqualTo("00000000002043975");
        assertThat(row.iur()).isEqualTo("9043f52d9111458baac55b7aac85f6ac");
        assertThat(row.importo()).isEqualTo(new BigDecimal("131"));
        assertThat(row.codEsito()).isZero();
        assertThat(row.dataEsitoSingoloPagamento()).isEqualTo(Instant.parse("2026-09-13T00:00:00Z"));
        assertThat(row.idsp()).isEqualTo(1);
        assertThat(row.idFlusso()).isEqualTo("2026-09-14ABI08811-0024220051715769");
        assertThat(row.dataOraFlusso()).isEqualTo(Instant.parse("2026-09-15T01:07:35Z"));
        assertThat(row.idDominio()).isEqualTo("00242200517");
        assertThat(row.psp()).isEqualTo("ABI08811");
        assertThat(row.intPsp()).isEqualTo("04774801007");
        assertThat(row.uniqueId()).isEqualTo("3cfb679a-3bfb-4c2d-9f50-5908de56d1bf");
        assertThat(row.insertedTimestamp())
                .isEqualTo(Instant.parse("2026-09-15T00:17:24.341577Z"));
    }

    /**
     * {@code COD_ESITO} e {@code IDSP} hanno un DEFAULT 0 a database, ma un campo vuoto deve
     * arrivarci come null: il default copre la riga che non porta la colonna, il null la riga che
     * la porta vuota. Rimpiazzarlo qui renderebbe i due casi indistinguibili, e zero e' un esito.
     */
    @Test
    void codEsitoEIdspVuotiRestanoNullENonZero() {
        List<String> fields = iuvFields();
        fields.set(3, "");
        fields.set(5, "");

        FdrIuvRow row = FdrIuvRowMapper.INSTANCE.map(iuvHeader, fields, DAY);

        assertThat(row.codEsito()).isNull();
        assertThat(row.idsp()).isNull();
    }

    @Test
    void unValoreNonConvertibileFallisceNominandoLaColonna() {
        List<String> fields = iuvFields();
        fields.set(2, "non-un-numero");

        assertThatThrownBy(() -> FdrIuvRowMapper.INSTANCE.map(iuvHeader, fields, DAY))
                .isInstanceOf(FdrValueException.class)
                .extracting(e -> ((FdrValueException) e).column())
                .isEqualTo(FdrColumns.IMPORTO);
    }

    // ─── Scelta del mapper ───────────────────────────────────────────────────────

    @Test
    void ilMapperVieneSceltoDalDataset() {
        assertThat(FdrRowMapper.forDataset(FdrDataset.FLUSSI_RENDICONTAZIONE))
                .isSameAs(FdrFlussoRowMapper.INSTANCE);
        assertThat(FdrRowMapper.forDataset(FdrDataset.IUV_RENDICONTATI))
                .isSameAs(FdrIuvRowMapper.INSTANCE);
    }

    // ─── Fixture ─────────────────────────────────────────────────────────────────

    private static List<String> flussiFields() {
        return new ArrayList<>(List.of(
                "2026-09-14IFSPIT21-CU20260000000WX4",
                "2026-09-14T22:00:00.000Z",
                "2026-09-15T08:38:01.7616300Z",
                "2026-09-14T00:00:00.000Z",
                "3604220120371326480320003200IT",
                "1",
                "34.51",
                "80001070673",
                "IFSPIT21",
                "14847241008",
                "50ea4624-ccbc-462a-96f3-5b2726f81ae3",
                "[\"2026-09-14T00:00:00.0000000Z\"]"));
    }

    private static List<String> iuvFields() {
        return new ArrayList<>(List.of(
                "00000000002043975",
                "9043f52d9111458baac55b7aac85f6ac",
                "131",
                "0",
                "2026-09-13T00:00:00Z",
                "1",
                "2026-09-14ABI08811-0024220051715769",
                "2026-09-15T01:07:35Z",
                "00242200517",
                "ABI08811",
                "04774801007",
                "3cfb679a-3bfb-4c2d-9f50-5908de56d1bf",
                "2026-09-15T00:17:24.341577Z"));
    }
}
