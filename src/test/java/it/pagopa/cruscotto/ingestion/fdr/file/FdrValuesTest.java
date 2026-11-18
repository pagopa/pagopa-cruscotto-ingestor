package it.pagopa.cruscotto.ingestion.fdr.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * La conversione dei valori: cosa viene accettato, cosa viene rifiutato, e cosa non viene mai
 * sostituito in silenzio.
 *
 * <p>Il principio e' uno solo: o il valore e' quello che la colonna dichiara, oppure la riga viene
 * contata come scartata. Nessuno zero al posto di un numero illeggibile, nessun epoch al posto di
 * una data illeggibile — sono le sostituzioni che arrivano in banca dati con l'aria di un dato
 * vero.
 */
class FdrValuesTest {

    // ─── Interi ──────────────────────────────────────────────────────────────────

    @Test
    void lInteroVieneLettoEIlCampoVuotoRestaNull() {
        assertThat(FdrValues.integer(FdrColumns.NUM_PAGAMENTI, "12")).isEqualTo(12);
        assertThat(FdrValues.integer(FdrColumns.NUM_PAGAMENTI, null)).isNull();
    }

    /**
     * Zero e' un esito valido di {@code COD_ESITO}, non un'assenza: non deve mai essere il valore
     * con cui si rimpiazza un campo illeggibile.
     */
    @Test
    void unInteroIllegibileNonDiventaZero() {
        assertThatThrownBy(() -> FdrValues.integer(FdrColumns.COD_ESITO, "OK"))
                .isInstanceOf(FdrValueException.class)
                .hasMessageContaining("OK")
                .extracting(e -> ((FdrValueException) e).column())
                .isEqualTo(FdrColumns.COD_ESITO);
    }

    // ─── Importi ─────────────────────────────────────────────────────────────────

    /**
     * La scala scritta dalla sorgente fa parte dell'importo. Un {@code double} arrotonderebbe in
     * ingresso e la banca dati conterrebbe un numero che il file non contiene.
     */
    @Test
    void lImportoConservaLaScalaDellaSorgente() {
        assertThat(FdrValues.decimal(FdrColumns.SOMMA_VERSATA, "34.51"))
                .isEqualTo(new BigDecimal("34.51"));
        assertThat(FdrValues.decimal(FdrColumns.IMPORTO, "70").scale()).isZero();
        assertThat(FdrValues.decimal(FdrColumns.IMPORTO, "33.50").scale()).isEqualTo(2);
    }

    /** La virgola decimale e' un'altra convenzione, non una variante: va vista, non indovinata. */
    @Test
    void laVirgolaDecimaleVieneRifiutata() {
        assertThatThrownBy(() -> FdrValues.decimal(FdrColumns.IMPORTO, "34,51"))
                .isInstanceOf(FdrValueException.class);
    }

    @Test
    void lImportoVuotoRestaNull() {
        assertThat(FdrValues.decimal(FdrColumns.IMPORTO, null)).isNull();
    }

    // ─── Timestamp ───────────────────────────────────────────────────────────────

    /**
     * Le sei forme osservate nei file di produzione: la parte frazionaria ha 0, 3, 4, 5, 6 o 7
     * cifre, a seconda del file e perfino della riga.
     */
    @ParameterizedTest
    @ValueSource(strings = {
            "2026-09-15T01:07:35Z",
            "2026-09-14T22:00:00.000Z",
            "2026-09-15T04:02:48.0000Z",
            "2026-09-15T04:02:48.00000Z",
            "2026-09-15T00:17:24.341577Z",
            "2026-09-15T08:38:01.7616300Z"})
    void leggeTutteLeFrazioniDiSecondoOsservate(String raw) {
        assertThat(FdrValues.timestamp(FdrColumns.INSERTED_TIMESTAMP, raw)).isNotNull();
    }

    /** La frazione arriva intera: la riduzione al microsecondo la fa PostgreSQL, non noi. */
    @Test
    void laFrazioneDiSecondoNonVienePerdutaInLettura() {
        assertThat(FdrValues.timestamp(FdrColumns.INSERTED_TIMESTAMP,
                "2026-09-15T08:38:01.7616300Z").getNano()).isEqualTo(761630000);
    }

    /**
     * L'orario e' quello scritto nel file, cifra per cifra. La colonna di destinazione e'
     * {@code TIMESTAMP WITHOUT TIME ZONE}: non c'e' nessun fuso da applicare, e applicarne uno
     * significherebbe scrivere in banca dati un'ora che nel sorgente non compare.
     */
    @Test
    void lOrarioEQuelloScrittoNelFile() {
        assertThat(FdrValues.timestamp(FdrColumns.DATA_ORA_FLUSSO, "2026-09-14T22:00:00.000Z"))
                .isEqualTo(LocalDateTime.of(2026, 9, 14, 22, 0));
    }

    /**
     * L'offset viene letto e scartato, mai applicato. Sui dati reali non cambia nulla - tutti i
     * valori portano {@code Z} - ma il giorno in cui arrivasse un {@code +02:00} la conversione
     * sposterebbe il valore di due ore in silenzio.
     */
    @Test
    void unOffsetEsplicitoNonSpostaLOrario() {
        assertThat(FdrValues.timestamp(FdrColumns.DATA_ORA_FLUSSO, "2026-09-15T00:00:00+02:00"))
                .isEqualTo(LocalDateTime.of(2026, 9, 15, 0, 0));
    }

    /**
     * E un valore senza fuso viene accettato, non rifiutato: e' una lettura come le altre, e la
     * colonna non potrebbe comunque conservare il fuso. Rifiutarlo costerebbe righe intere per un
     * dettaglio che la destinazione non ha.
     */
    @Test
    void unTimestampSenzaFusoVieneAccettatoEConservato() {
        assertThat(FdrValues.timestamp(FdrColumns.INSERTED_TIMESTAMP, "2026-09-15T08:38:01"))
                .isEqualTo(LocalDateTime.of(2026, 9, 15, 8, 38, 1));
    }

    @Test
    void unFormatoNonIsoVieneRifiutato() {
        assertThatThrownBy(() ->
                FdrValues.timestamp(FdrColumns.INSERTED_TIMESTAMP, "15/09/2026 08:38:01"))
                .isInstanceOf(FdrValueException.class);
    }

    @Test
    void ilTimestampVuotoRestaNull() {
        assertThat(FdrValues.timestamp(FdrColumns.DATA_REGOLAMENTO, null)).isNull();
    }

    /** Anche queste sono controllo di flusso: niente stack trace per riga scartata. */
    @Test
    void lEccezioneDiValoreNonPortaStackTrace() {
        assertThatThrownBy(() -> FdrValues.integer(FdrColumns.IDSP, "x"))
                .satisfies(e -> assertThat(e.getStackTrace()).isEmpty());
    }
}
