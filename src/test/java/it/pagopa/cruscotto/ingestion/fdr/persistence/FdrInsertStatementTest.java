package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.fdr.file.FdrFlussoRow;
import it.pagopa.cruscotto.ingestion.fdr.file.FdrIuvRow;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Il mapping verso i parametri SQL, colonna per colonna.
 *
 * <p>Le tre perdite di informazione raggiungibili per sbaglio con JDBC sono tutte sotto test: un
 * Integer nullo che diventa zero, un istante che passa per un fuso orario, un importo che passa
 * per un double.
 */
class FdrInsertStatementTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    // ─── SQL ─────────────────────────────────────────────────────────────────────

    /**
     * L'ID viene dalla sequence dentro lo statement, come fa il bulk writer AS-IS per ogni entita':
     * nessun contatore in memoria che possa disallinearsi dal database, e niente da riconciliare
     * dopo un batch fallito.
     */
    @Test
    void lSqlPrendeLIdDallaSequenceENonDaUnContatore() {
        assertThat(FdrFlussoInsert.INSTANCE.sql("sert_ingestor"))
                .contains("nextval('sert_ingestor.SQ_FDR_FLUSSI_RENDICONTAZIONE')")
                .contains("INSERT INTO sert_ingestor.FDR_FLUSSI_RENDICONTAZIONE");
        assertThat(FdrIuvInsert.INSTANCE.sql("sert_ingestor"))
                .contains("nextval('sert_ingestor.SQ_FDR_IUV_RENDICONTATI')");
    }

    /**
     * Nessun ON CONFLICT. I file reali contengono righe duplicate identiche - 14 in una giornata di
     * flussi - e devono arrivare in banca dati: un duplicato che si vede e' un difetto sorgente
     * diagnosticabile, uno eliminato in silenzio non lo e'.
     */
    @Test
    void nessunoDeiDueInsertDeduplica() {
        assertThat(FdrFlussoInsert.INSTANCE.sql("s"))
                .doesNotContainIgnoringCase("ON CONFLICT")
                .doesNotContainIgnoringCase("MERGE");
        assertThat(FdrIuvInsert.INSTANCE.sql("s"))
                .doesNotContainIgnoringCase("ON CONFLICT")
                .doesNotContainIgnoringCase("MERGE");
    }

    @Test
    void ilDispatchVerificaIlDataset() {
        assertThat(FdrInsertStatement.forDataset(FdrDataset.FLUSSI_RENDICONTAZIONE))
                .isSameAs(FdrFlussoInsert.INSTANCE);
        assertThat(FdrInsertStatement.forDataset(FdrDataset.IUV_RENDICONTATI))
                .isSameAs(FdrIuvInsert.INSTANCE);
    }

    // ─── FLUSSI ──────────────────────────────────────────────────────────────────

    @Test
    void mappaTutteLeColonneDiFlussi() throws SQLException {
        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE, flusso());

        assertThat(bound.get(1)).isEqualTo(Date.valueOf(DAY));
        assertThat(bound.get(2)).isEqualTo(LocalDateTime.of(2026, 9, 15, 8, 38, 1, 761630000));
        assertThat(bound.get(3)).isEqualTo("2026-09-14IFSPIT21-CU20260000000WX4");
        assertThat(bound.get(4)).isEqualTo(LocalDateTime.of(2026, 9, 14, 22, 0));
        assertThat(bound.get(5)).isEqualTo(LocalDateTime.of(2026, 9, 14, 0, 0));
        assertThat(bound.get(6)).isEqualTo("3604220120371326480320003200IT");
        assertThat(bound.get(7)).isEqualTo(1);
        assertThat(bound.get(8)).isEqualTo(new BigDecimal("34.51"));
        assertThat(bound.get(9)).isEqualTo("80001070673");
        assertThat(bound.get(10)).isEqualTo("IFSPIT21");
        assertThat(bound.get(11)).isEqualTo("14847241008");
        assertThat(bound.get(12)).isEqualTo("50ea4624-ccbc-462a-96f3-5b2726f81ae3");
        assertThat(bound.get(13)).isEqualTo("[\"2026-09-14T00:00:00.0000000Z\"]");
    }

    /**
     * ALL_DATES va in colonna com'e'. La sua semantica di business non e' certificata, quindi
     * qualunque parsing sarebbe un'interpretazione nostra applicata in modo distruttivo.
     */
    @Test
    void allDatesVienePersistitoGrezzo() throws SQLException {
        String raw = "[\"2026-09-14T00:00:00Z\",\"2026-09-11T00:00:00Z\"]";
        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE,
                withAllDates(flusso(), raw));

        assertThat(bound.get(13)).isEqualTo(raw);
    }

    @Test
    void leColonneNullabiliDiFlussiVannoANull() throws SQLException {
        FdrFlussoRow vuota = new FdrFlussoRow(DAY, null, null, null, null, null, null, null, null,
                null, null, null, null);

        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE, vuota);

        assertThat(bound.get(1)).isEqualTo(Date.valueOf(DAY));
        assertThat(bound).containsEntry(2, null).containsEntry(7, null).containsEntry(8, null)
                .containsEntry(13, null);
    }

    // ─── IUV ─────────────────────────────────────────────────────────────────────

    @Test
    void mappaTutteLeColonneDiIuv() throws SQLException {
        Map<Integer, Object> bound = BoundValues.of(FdrIuvInsert.INSTANCE, iuv());

        assertThat(bound.get(1)).isEqualTo(Date.valueOf(DAY));
        assertThat(bound.get(2)).isEqualTo("00000000002043975");
        assertThat(bound.get(3)).isEqualTo("9043f52d9111458baac55b7aac85f6ac");
        assertThat(bound.get(4)).isEqualTo(new BigDecimal("131"));
        assertThat(bound.get(5)).isEqualTo(0);
        assertThat(bound.get(6)).isEqualTo(LocalDateTime.of(2026, 9, 13, 0, 0));
        assertThat(bound.get(7)).isEqualTo(1);
        assertThat(bound.get(8)).isEqualTo("2026-09-14ABI08811-0024220051715769");
        assertThat(bound.get(9)).isEqualTo(LocalDateTime.of(2026, 9, 15, 1, 7, 35));
        assertThat(bound.get(10)).isEqualTo("00242200517");
        assertThat(bound.get(11)).isEqualTo("ABI08811");
        assertThat(bound.get(12)).isEqualTo("04774801007");
        assertThat(bound.get(13)).isEqualTo("3cfb679a-3bfb-4c2d-9f50-5908de56d1bf");
        assertThat(bound.get(14)).isEqualTo(LocalDateTime.of(2026, 9, 15, 0, 17, 24, 341577000));
    }

    /**
     * Lo IUV e' una stringa e deve restarlo: arriva con gli zeri iniziali e puo' essere
     * alfanumerico, RF compreso. Qualunque conversione numerica mangerebbe gli zeri e romperebbe
     * tutte le ricerche su cui il backend e' costruito.
     */
    @ParameterizedTest
    @CsvSource({"00000000002043975", "RF18000000000539900", "A1B2C300000000000"})
    void loIuvRestaStringaConGliZeriIniziali(String value) throws SQLException {
        Map<Integer, Object> bound = BoundValues.of(FdrIuvInsert.INSTANCE, withIuv(iuv(), value));

        assertThat(bound.get(2)).isInstanceOf(String.class).isEqualTo(value);
    }

    /**
     * COD_ESITO e IDSP hanno un DEFAULT 0 a database, ma un campo assente deve arrivare come SQL
     * null: il default copre la riga che non porta la colonna, il null la riga che la porta vuota.
     * E zero e' un esito valido, non un'assenza.
     */
    @Test
    void codEsitoEIdspAssentiVannoANullENonAZero() throws SQLException {
        FdrIuvRow senza = new FdrIuvRow(DAY, "00000000002043975", "iur", null, null, null, null,
                "flusso", null, null, null, null, null, null);

        Map<Integer, Object> bound = BoundValues.of(FdrIuvInsert.INSTANCE, senza);

        assertThat(bound).containsEntry(5, null).containsEntry(7, null);
    }

    // ─── Tipi ────────────────────────────────────────────────────────────────────

    /**
     * Le frazioni di secondo osservate nei file: 0, 3, 6 e 7 cifre. Fino a qui sono conservate per
     * intero; la riduzione al microsecondo avviene a valle, nel driver e in PostgreSQL, ed e'
     * documentata nel mapping.
     */
    @ParameterizedTest
    @CsvSource({
            "2026-09-15T01:07:35Z, 0",
            "2026-09-14T22:00:00.123Z, 123000000",
            "2026-09-15T00:17:24.341577Z, 341577000",
            "2026-09-15T08:38:01.7616300Z, 761630000"})
    void leFrazioniDiSecondoArrivanoIntereAlBinding(String raw, int expectedNanos)
            throws SQLException {
        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE,
                withInsertedTimestamp(flusso(), OffsetDateTime.parse(raw).toLocalDateTime()));

        assertThat(bound.get(2)).isInstanceOf(LocalDateTime.class);
        assertThat(((LocalDateTime) bound.get(2)).getNano()).isEqualTo(expectedNanos);
    }

    /**
     * La conversione e' esplicitamente a UTC, non al fuso della macchina: altrimenti lo stesso file
     * darebbe timestamp diversi a seconda di dove gira l'ingestor, e si sposterebbe di un'ora due
     * volte l'anno.
     */
    @Test
    void ilTimestampVieneConvertitoAUtcENonAlFusoLocale() throws SQLException {
        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE,
                withInsertedTimestamp(flusso(), LocalDateTime.parse("2026-07-15T23:30:00")));

        assertThat(bound.get(2)).isEqualTo(LocalDateTime.of(2026, 7, 15, 23, 30));
    }

    /** DATE_INSERTED e' un giorno di calendario: nessun fuso orario partecipa alla conversione. */
    @Test
    void laGiornataVieneBindataComeDataSenzaOrario() throws SQLException {
        Map<Integer, Object> bound = BoundValues.of(FdrFlussoInsert.INSTANCE, flusso());

        assertThat(bound.get(1)).isInstanceOf(Date.class);
        assertThat(bound.get(1).toString()).isEqualTo("2026-09-15");
    }

    /**
     * La scala scritta dalla sorgente fa parte dell'importo: un double arrotonderebbe in ingresso e
     * la colonna NUMERIC conterrebbe un numero che il file non contiene.
     */
    @ParameterizedTest
    @CsvSource({"0", "0.00", "34.51", "1234567890.123456", "70", "33.50"})
    void lImportoArrivaComeBigDecimalConLaScalaOriginale(String amount) throws SQLException {
        Map<Integer, Object> bound = BoundValues.of(FdrIuvInsert.INSTANCE,
                withImporto(iuv(), new BigDecimal(amount)));

        assertThat(bound.get(4)).isInstanceOf(BigDecimal.class)
                .isEqualTo(new BigDecimal(amount));
        assertThat(((BigDecimal) bound.get(4)).toPlainString()).isEqualTo(amount);
    }

    // ─── Fixture ─────────────────────────────────────────────────────────────────

    static FdrFlussoRow flusso() {
        return new FdrFlussoRow(DAY,
                LocalDateTime.parse("2026-09-15T08:38:01.7616300"),
                "2026-09-14IFSPIT21-CU20260000000WX4",
                LocalDateTime.parse("2026-09-14T22:00:00"),
                LocalDateTime.parse("2026-09-14T00:00:00"),
                "3604220120371326480320003200IT",
                1,
                new BigDecimal("34.51"),
                "80001070673",
                "IFSPIT21",
                "14847241008",
                "50ea4624-ccbc-462a-96f3-5b2726f81ae3",
                "[\"2026-09-14T00:00:00.0000000Z\"]");
    }

    static FdrIuvRow iuv() {
        return new FdrIuvRow(DAY,
                "00000000002043975",
                "9043f52d9111458baac55b7aac85f6ac",
                new BigDecimal("131"),
                0,
                LocalDateTime.parse("2026-09-13T00:00:00"),
                1,
                "2026-09-14ABI08811-0024220051715769",
                LocalDateTime.parse("2026-09-15T01:07:35"),
                "00242200517",
                "ABI08811",
                "04774801007",
                "3cfb679a-3bfb-4c2d-9f50-5908de56d1bf",
                LocalDateTime.parse("2026-09-15T00:17:24.341577"));
    }

    private static FdrFlussoRow withAllDates(FdrFlussoRow row, String allDates) {
        return new FdrFlussoRow(row.dateInserted(), row.insertedTimestamp(), row.idFlusso(),
                row.dataOraFlusso(), row.dataRegolamento(), row.causale(), row.numPagamenti(),
                row.sommaVersata(), row.idDominio(), row.psp(), row.intPsp(), row.uniqueId(),
                allDates);
    }

    private static FdrFlussoRow withInsertedTimestamp(FdrFlussoRow row, LocalDateTime timestamp) {
        return new FdrFlussoRow(row.dateInserted(), timestamp, row.idFlusso(), row.dataOraFlusso(),
                row.dataRegolamento(), row.causale(), row.numPagamenti(), row.sommaVersata(),
                row.idDominio(), row.psp(), row.intPsp(), row.uniqueId(), row.allDates());
    }

    private static FdrIuvRow withIuv(FdrIuvRow row, String value) {
        return new FdrIuvRow(row.dateInserted(), value, row.iur(), row.importo(), row.codEsito(),
                row.dataEsitoSingoloPagamento(), row.idsp(), row.idFlusso(), row.dataOraFlusso(),
                row.idDominio(), row.psp(), row.intPsp(), row.uniqueId(), row.insertedTimestamp());
    }

    private static FdrIuvRow withImporto(FdrIuvRow row, BigDecimal importo) {
        return new FdrIuvRow(row.dateInserted(), row.iuv(), row.iur(), importo, row.codEsito(),
                row.dataEsitoSingoloPagamento(), row.idsp(), row.idFlusso(), row.dataOraFlusso(),
                row.idDominio(), row.psp(), row.intPsp(), row.uniqueId(), row.insertedTimestamp());
    }

    /**
     * INGESTED_AT non viene bindata: la riempie il {@code DEFAULT now()} della colonna.
     *
     * <p>È una scelta, non una dimenticanza, e vale la pena ancorarla perché l'istinto di chi
     * aggiunge una colonna è aggiungerla anche qui. Lasciarla al database costa un parametro in
     * meno per riga su centinaia di migliaia di righe al giorno, e soprattutto nessun percorso di
     * inserimento — applicativo o manuale, presente o futuro — può dimenticarsene.
     *
     * <p>C'è anche un effetto che la rende più utile così: {@code now()} è l'ora di <em>inizio
     * transazione</em>, quindi tutte le righe di uno stesso batch condividono l'istante. È proprio
     * il raggruppamento che serve a riconoscere a colpo d'occhio due ingestioni diverse della
     * stessa giornata.
     */
    @Test
    void lSqlNonBindaIngestedAtMaLaLasciaAlDefault() {
        for (FdrInsertStatement statement : List.of(FdrFlussoInsert.INSTANCE,
                FdrIuvInsert.INSTANCE)) {
            assertThat(statement.sql("sert_ingestor"))
                    .as("la colonna la riempie il default della tabella, non lo statement")
                    .doesNotContain("INGESTED_AT");
        }
    }
}
