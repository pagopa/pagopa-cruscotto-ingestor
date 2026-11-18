package it.pagopa.cruscotto.ingestion.fdr.file;

import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * L'header, cioe' il punto in cui si decide se il file e' ancora quello concordato.
 *
 * <p>La garanzia che conta e' la lettura <strong>per nome</strong>: i due dataset condividono otto
 * nomi di colonna in posizioni diverse, quindi un lettore posizionale a cui arrivi un file con le
 * colonne riordinate non fallirebbe — scriverebbe uno IUR dentro uno IUV.
 */
class FdrCsvHeaderTest {

    private static final List<String> FLUSSI = List.of(
            "ID_FLUSSO", "DATA_ORA_FLUSSO", "INSERTED_TIMESTAMP", "DATA_REGOLAMENTO", "CAUSALE",
            "NUM_PAGAMENTI", "SOMMA_VERSATA", "ID_DOMINIO", "PSP", "INT_PSP", "UNIQUE_ID",
            "ALL_DATES");

    private static final List<String> IUV = List.of(
            "IUV", "IUR", "IMPORTO", "COD_ESITO", "DATA_ESITO_SINGOLO_PAGAMENTO", "IDSP",
            "ID_FLUSSO", "DATA_ORA_FLUSSO", "ID_DOMINIO", "PSP", "INT_PSP", "UNIQUE_ID",
            "INSERTED_TIMESTAMP");

    // ─── Gli header reali ────────────────────────────────────────────────────────

    @Test
    void gliHeaderRealiDeiDueDatasetSonoAccettati() {
        assertThat(FdrCsvHeader.of(FLUSSI, FdrDataset.FLUSSI_RENDICONTAZIONE).columnCount())
                .isEqualTo(12);
        assertThat(FdrCsvHeader.of(IUV, FdrDataset.IUV_RENDICONTATI).columnCount()).isEqualTo(13);
    }

    /**
     * La prova che la lettura e' per nome. Invertendo due colonne il valore deve seguire il nome:
     * se seguisse la posizione, questo test passerebbe comunque e il difetto resterebbe invisibile
     * proprio sulle due colonne che si somigliano di piu'.
     */
    @Test
    void leColonneScambiateNonScambianoIValori() {
        List<String> swapped = new ArrayList<>(IUV);
        swapped.set(0, "IUR");
        swapped.set(1, "IUV");
        FdrCsvHeader header = FdrCsvHeader.of(swapped, FdrDataset.IUV_RENDICONTATI);

        List<String> fields = new ArrayList<>(List.of("valore-iur", "valore-iuv"));
        while (fields.size() < swapped.size()) {
            fields.add("");
        }

        assertThat(header.value(fields, FdrColumns.IUV)).isEqualTo("valore-iuv");
        assertThat(header.value(fields, FdrColumns.IUR)).isEqualTo("valore-iur");
    }

    @Test
    void ilConfrontoIgnoraMaiuscoleESpaziNellHeader() {
        List<String> noisy = FLUSSI.stream().map(c -> " " + c.toLowerCase() + " ").toList();

        assertThat(FdrCsvHeader.of(noisy, FdrDataset.FLUSSI_RENDICONTAZIONE).unexpectedColumns())
                .isEmpty();
    }

    // ─── Header che non corrisponde ──────────────────────────────────────────────

    /**
     * Una colonna mancante non e' rilevabile riga per riga: andando avanti si scriverebbe una
     * giornata intera con quel campo a null. Deve fermare il file mentre i dati sono ancora sul
     * blob, e il messaggio deve dire quale colonna, altrimenti serve comunque aprire il file.
     */
    @Test
    void unaColonnaMancanteFermaIlFileENeDiceIlNome() {
        List<String> missing = new ArrayList<>(FLUSSI);
        missing.remove("CAUSALE");

        assertThatThrownBy(() -> FdrCsvHeader.of(missing, FdrDataset.FLUSSI_RENDICONTAZIONE))
                .isInstanceOf(FdrFileFormatException.class)
                .hasMessageContaining("CAUSALE")
                .extracting(e -> ((FdrFileFormatException) e).kind())
                .isEqualTo(FdrFileFormatException.Kind.UNEXPECTED_HEADER);
    }

    /** Due colonne con lo stesso nome: nulla puo' dire quale delle due porti il dato. */
    @Test
    void unaColonnaDuplicataFermaIlFile() {
        List<String> duplicated = new ArrayList<>(FLUSSI);
        duplicated.add("CAUSALE");

        assertThatThrownBy(() -> FdrCsvHeader.of(duplicated, FdrDataset.FLUSSI_RENDICONTAZIONE))
                .isInstanceOf(FdrFileFormatException.class)
                .hasMessageContaining("duplicated")
                .hasMessageContaining("CAUSALE");
    }

    /**
     * Una colonna in piu' invece non ferma nulla: la sorgente e' libera di aggiungere un campo, e
     * perdere una giornata per una colonna che nessuno legge sarebbe il danno peggiore. Viene
     * riportata, cosi' l'aggiunta si nota.
     */
    @Test
    void unaColonnaInPiuVieneAccettataERiportata() {
        List<String> extra = new ArrayList<>(FLUSSI);
        extra.add("NUOVA_COLONNA");

        FdrCsvHeader header = FdrCsvHeader.of(extra, FdrDataset.FLUSSI_RENDICONTAZIONE);

        assertThat(header.unexpectedColumns()).containsExactly("NUOVA_COLONNA");
        assertThat(header.columnCount()).isEqualTo(13);
    }

    @Test
    void unHeaderDiAltroDatasetNonPassaPerQuestoDataset() {
        assertThatThrownBy(() -> FdrCsvHeader.of(IUV, FdrDataset.FLUSSI_RENDICONTAZIONE))
                .isInstanceOf(FdrFileFormatException.class);
    }

    // ─── Lettura dei valori ──────────────────────────────────────────────────────

    @Test
    void ilCampoVuotoDiventaNull() {
        FdrCsvHeader header = FdrCsvHeader.of(FLUSSI, FdrDataset.FLUSSI_RENDICONTAZIONE);
        List<String> fields = row(12);
        fields.set(4, "");

        assertThat(header.value(fields, FdrColumns.CAUSALE)).isNull();
    }

    @Test
    void ilValoreNonVieneRipulito() {
        FdrCsvHeader header = FdrCsvHeader.of(FLUSSI, FdrDataset.FLUSSI_RENDICONTAZIONE);
        List<String> fields = row(12);
        fields.set(4, "  causale con spazi  ");

        assertThat(header.value(fields, FdrColumns.CAUSALE)).isEqualTo("  causale con spazi  ");
    }

    /**
     * Una riga piu' corta dell'header non viene letta a meta'. Leggere le colonne presenti
     * significherebbe fidarsi della loro posizione, che e' esattamente cio' che una riga troncata
     * mette in dubbio.
     */
    @Test
    void unaRigaPiuCortaDellHeaderFallisceInveceDiLeggereAMeta() {
        FdrCsvHeader header = FdrCsvHeader.of(FLUSSI, FdrDataset.FLUSSI_RENDICONTAZIONE);

        assertThatThrownBy(() -> header.value(row(3), FdrColumns.ALL_DATES))
                .isInstanceOf(FdrValueException.class)
                .hasMessageContaining("3 fields")
                .extracting(e -> ((FdrValueException) e).column())
                .isEqualTo(FdrColumns.ALL_DATES);
    }

    @Test
    void chiedereUnaColonnaCheLHeaderNonHaEUnErrore() {
        FdrCsvHeader header = FdrCsvHeader.of(FLUSSI, FdrDataset.FLUSSI_RENDICONTAZIONE);

        assertThatThrownBy(() -> header.value(row(12), FdrColumns.IUV))
                .isInstanceOf(FdrValueException.class);
    }

    private static List<String> row(int size) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            fields.add("v" + i);
        }
        return fields;
    }
}
