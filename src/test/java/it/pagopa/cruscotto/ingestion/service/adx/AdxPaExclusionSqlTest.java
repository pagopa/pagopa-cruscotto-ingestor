package it.pagopa.cruscotto.ingestion.service.adx;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Esclusione degli enti creditori dall'ingestion, applicata su ADX.
 *
 * <p>Le due proprieta' che questi test proteggono sono entrambe "silenziose" se si rompono: una
 * lista mal formata non deve poter alterare la query, e una riga senza PA non deve sparire.</p>
 */
class AdxPaExclusionSqlTest {

    @Test
    void listaVuotaNonFiltraNulla() {
        assertEquals("", AdxPaExclusionSql.clause(null));
        assertEquals("", AdxPaExclusionSql.clause(List.of()));
    }

    @Test
    void produceUnaClausolaConTuttiICodici() {
        String clause = AdxPaExclusionSql.clause(List.of("77777777777", "12345678901"));

        assertTrue(clause.contains("\"77777777777\""), clause);
        assertTrue(clause.contains("\"12345678901\""), clause);
        // !in e non == negato: le righe con PA vuota devono passare.
        assertTrue(clause.contains("!in ("), clause);
        assertTrue(clause.startsWith("\n| where"), "va su una riga propria dopo il bound temporale: " + clause);
    }

    /**
     * I codici finiscono nella query per concatenazione — in KQL non esistono bind parameter —
     * quindi la validazione e' obbligatoria, non difensiva.
     */
    @Test
    void scartaIValoriCheRenderebberoLaQueryInterpolabile() {
        String clause = AdxPaExclusionSql.clause(List.of(
                "77777777777",
                "\") or true //",
                "123; drop",
                "abc"));

        assertTrue(clause.contains("\"77777777777\""), clause);
        assertFalse(clause.contains("or true"), "valore non numerico interpolato: " + clause);
        assertFalse(clause.contains("drop"), "valore non numerico interpolato: " + clause);
        assertFalse(clause.contains("abc"), "valore non numerico interpolato: " + clause);
        assertEquals(List.of("\"77777777777\""), codiciNellaLista(clause), clause);
    }

    /** Se nessun codice supera la validazione la clausola sparisce: meglio non filtrare che filtrare a caso. */
    @Test
    void seNessunCodiceEValidoNonSiFiltra() {
        assertEquals("", AdxPaExclusionSql.clause(List.of("abc", "", "  ")));
    }

    @Test
    void ignoraSpaziEDuplicati() {
        String clause = AdxPaExclusionSql.clause(Arrays.asList(" 77777777777 ", "77777777777", null, ""));

        assertEquals(List.of("\"77777777777\""), codiciNellaLista(clause),
                "il codice deve comparire una volta sola: " + clause);
    }

    /**
     * La proprieta' e' una stringa in {@code application.yml} e un {@code List<String>} nel codice:
     * la conversione la fa Spring, e <strong>nessun altro test la esercita</strong> perche' la suite
     * non avvia un contesto. Se non funzionasse, l'esclusione sarebbe silenziosamente inattiva — il
     * modo peggiore di fallire, perche' sembrerebbe configurata.
     */
    @Test
    void laProprietaConfiguratavieneConvertitaInListaDaSpring() {
        assertEquals(List.of("77777777777"), bindExcluded("77777777777"));
        assertEquals(List.of("77777777777", "12345678901"), bindExcluded("77777777777,12345678901"));
    }

    /** Valore vuoto = nessuna esclusione, ed e' il modo documentato per disattivarla. */
    @Test
    void unValoreVuotoDisattivaLEsclusione() {
        List<String> bound = bindExcluded("");

        assertTrue(bound.isEmpty() || bound.stream().allMatch(String::isBlank), String.valueOf(bound));
        assertEquals("", AdxPaExclusionSql.clause(bound));
    }

    private static List<String> bindExcluded(String rawValue) {
        Binder binder = new Binder(new MapConfigurationPropertySource(
                Map.of("ingestion.adx.excluded-pa-emittenti", rawValue)));
        return binder.bind("ingestion.adx.excluded-pa-emittenti", Bindable.listOf(String.class))
                .orElse(List.of());
    }

    /**
     * Estrae i soli elementi dentro {@code !in (...)}.
     *
     * <p>Contare le virgolette sull'intera clausola non funziona: ce ne sono anche nel
     * {@code trim(" ", ...)}. Il primo tentativo di questo test lo faceva, e falliva pur essendo
     * corretto il codice sotto test.</p>
     */
    private static List<String> codiciNellaLista(String clause) {
        int open = clause.indexOf("!in (");
        if (open < 0) {
            return List.of();
        }
        String inner = clause.substring(open + "!in (".length(), clause.lastIndexOf(')'));
        return Arrays.stream(inner.split(",")).map(String::trim).toList();
    }

    /**
     * Il confronto avviene su {@code trim(" ", tostring(PA_EMITTENTE))}: lo stesso idioma usato dagli
     * altri filtri dei template, perche' il valore sorgente puo' arrivare con spazi.
     */
    @Test
    void confrontaSulValoreNormalizzatoComeGliAltriFiltri() {
        String clause = AdxPaExclusionSql.clause(List.of("77777777777"));

        assertTrue(clause.contains("trim(\" \", tostring(PA_EMITTENTE))"), clause);
    }
}
