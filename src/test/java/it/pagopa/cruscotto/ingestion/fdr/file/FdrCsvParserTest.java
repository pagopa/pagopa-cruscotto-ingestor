package it.pagopa.cruscotto.ingestion.fdr.file;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Le regole di quoting, una per una.
 *
 * <p>Il separatore e la forma del CSV non sono assunzioni: vengono dal profilo di 810.492 righe
 * reali esportate dalla produzione. I casi di questo test sono quelli osservati li' dentro — in
 * particolare il quoting <em>misto</em>, che e' l'unico in grado di far leggere una colonna al
 * posto di un'altra senza che nulla fallisca.
 */
class FdrCsvParserTest {

    /** Riga reale di FLUSSI: tutto quotato, e l'ultimo campo e' un array JSON con gli apici doppi. */
    private static final String FLUSSI_LINE =
            "\"2026-09-14IFSPIT21-CU20260000000WX4\",\"2026-09-14T22:00:00.000Z\","
                    + "\"2026-09-15T08:38:01.7616300Z\",\"2026-09-14T00:00:00.000Z\","
                    + "\"3604220120371326480320003200IT\",\"1\",\"34.51\",\"80001070673\","
                    + "\"IFSPIT21\",\"14847241008\",\"50ea4624-ccbc-462a-96f3-5b2726f81ae3\","
                    + "\"[\"\"2026-09-14T00:00:00.0000000Z\"\"]\"";

    /** Riga reale di IUV nella forma a quoting misto: {@code COD_ESITO} arriva nudo. */
    private static final String IUV_MIXED_LINE =
            "\"00000000002043975\",\"9043f52d9111458baac55b7aac85f6ac\",\"131\",0,"
                    + "\"2026-09-13T00:00:00Z\",\"1\",\"2026-09-14ABI08811-0024220051715769\","
                    + "\"2026-09-15T01:07:35Z\",\"00242200517\",\"ABI08811\",\"04774801007\","
                    + "\"3cfb679a-3bfb-4c2d-9f50-5908de56d1bf\",\"2026-09-15T00:17:24.341577Z\"";

    // ─── Le forme osservate in produzione ────────────────────────────────────────

    @Test
    void leggeLaRigaRealeDiFlussiConDodiciCampi() {
        List<String> fields = FdrCsvParser.parseLine(FLUSSI_LINE);

        assertThat(fields).hasSize(12);
        assertThat(fields.get(0)).isEqualTo("2026-09-14IFSPIT21-CU20260000000WX4");
        assertThat(fields.get(6)).isEqualTo("34.51");
        assertThat(fields.get(10)).isEqualTo("50ea4624-ccbc-462a-96f3-5b2726f81ae3");
    }

    /**
     * Il campo {@code ALL_DATES} contiene virgole e apici doppi raddoppiati: e' la ragione per cui
     * uno {@code split(",")} non basta, e il motivo per cui il valore resta testo grezzo.
     */
    @Test
    void ilCampoConArrayJsonResistealleVirgoleEAgliApiciRaddoppiati() {
        String line = "\"a\",\"[\"\"2026-09-14T00:00:00Z\"\",\"\"2026-09-11T00:00:00Z\"\"]\"";

        List<String> fields = FdrCsvParser.parseLine(line);

        assertThat(fields).containsExactly("a",
                "[\"2026-09-14T00:00:00Z\",\"2026-09-11T00:00:00Z\"]");
    }

    /**
     * IL caso che decide la forma del parser. Nello stesso giorno, la stessa colonna arriva quotata
     * in un file e nuda in un altro: un parser che assumesse il quoting uniforme non fallirebbe,
     * disallineerebbe i campi — ed e' il difetto che arriva in banca dati con l'aria di essere
     * plausibile.
     */
    @Test
    void leggeLaRigaRealeDiIuvConQuotingMisto() {
        List<String> fields = FdrCsvParser.parseLine(IUV_MIXED_LINE);

        assertThat(fields).hasSize(13);
        assertThat(fields.get(3)).isEqualTo("0");
        assertThat(fields.get(12)).isEqualTo("2026-09-15T00:17:24.341577Z");
    }

    // ─── Campi vuoti e bordi ─────────────────────────────────────────────────────

    @Test
    void ilCampoVuotoRestaStringaVuotaENonNull() {
        assertThat(FdrCsvParser.parseLine("a,,b")).containsExactly("a", "", "b");
        assertThat(FdrCsvParser.parseLine("a,\"\",b")).containsExactly("a", "", "b");
    }

    /**
     * Una riga che finisce col separatore ha un ultimo campo vuoto, non un campo in meno: il
     * conteggio dei campi e' cio' su cui si decide se una riga e' troncata.
     */
    @Test
    void ilSeparatoreFinaleProduceUnUltimoCampoVuoto() {
        assertThat(FdrCsvParser.parseLine("a,b,")).containsExactly("a", "b", "");
    }

    @Test
    void laRigaVuotaProduceUnSoloCampoVuoto() {
        assertThat(FdrCsvParser.parseLine("")).containsExactly("");
    }

    @Test
    void gliSpaziNonVengonoRimossi() {
        assertThat(FdrCsvParser.parseLine(" a , b ")).containsExactly(" a ", " b ");
    }

    // ─── Apici in posizioni anomale ──────────────────────────────────────────────

    /**
     * L'unico caso che il parser non puo' risolvere da solo: o il file usa campi multi-riga —
     * nessuna delle 810.492 righe profilate lo fa — oppure la riga e' corrotta. Indovinare
     * sposterebbe di una posizione tutti i campi successivi.
     */
    @Test
    void unApiceNonChiusoFaFallireLaRiga() {
        assertThatThrownBy(() -> FdrCsvParser.parseLine("\"a\",\"b"))
                .isInstanceOf(FdrMalformedLineException.class)
                .hasMessageContaining("unterminated");
    }

    /** L'eccezione di riga non porta stack trace: e' controllo di flusso su file da 600.000 righe. */
    @Test
    void lEccezioneDiRigaNonPortaStackTrace() {
        assertThatThrownBy(() -> FdrCsvParser.parseLine("\"a"))
                .isInstanceOf(FdrMalformedLineException.class)
                .satisfies(e -> assertThat(e.getStackTrace()).isEmpty());
    }

    @Test
    void unApiceDentroUnCampoNonQuotatoResta() {
        assertThat(FdrCsvParser.parseLine("a\"b,c")).containsExactly("a\"b", "c");
    }

    @Test
    void ilTestoDopoLApiceDiChiusuraSiUnisceAlloStessoCampo() {
        assertThat(FdrCsvParser.parseLine("\"a\"b,c")).containsExactly("ab", "c");
    }

    // ─── BOM ─────────────────────────────────────────────────────────────────────

    /**
     * Il BOM c'e' in alcuni file e non in altri, nello stesso giorno e nello stesso dataset. Senza
     * rimuoverlo la prima colonna dell'header si chiamerebbe {@code ﻿ID_FLUSSO} e non
     * corrisponderebbe a nessun nome atteso: il file fallirebbe per un carattere invisibile.
     */
    @Test
    void ilBomVieneRimossoSoloInTestaERestituisceLaStringaAltrimenti() {
        assertThat(FdrCsvParser.stripBom("﻿\"ID_FLUSSO\"")).isEqualTo("\"ID_FLUSSO\"");
        assertThat(FdrCsvParser.stripBom("\"ID_FLUSSO\"")).isEqualTo("\"ID_FLUSSO\"");
        assertThat(FdrCsvParser.stripBom("a﻿b")).isEqualTo("a﻿b");
        assertThat(FdrCsvParser.stripBom("")).isEmpty();
        assertThat(FdrCsvParser.stripBom(null)).isNull();
    }
}
