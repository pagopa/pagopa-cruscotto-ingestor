package it.pagopa.cruscotto.ingestion.fdr.persistence;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L'unico troncamento autorizzato di FDR.
 *
 * <p>Il cliente ha scelto di tenere la colonna a 30 e accorciare il valore: sul 21,71% delle righe
 * di una giornata reale la causale viene persa in parte, ed e' il comportamento richiesto. Qui si
 * verifica dove cade esattamente il taglio, perche' un carattere in piu' farebbe fallire l'INSERT
 * e uno in meno butterebbe via un carattere che la colonna avrebbe accettato.
 */
class FdrCausaleTest {

    @Test
    void sottoSogliaIlValoreNonVieneToccato() {
        String ventinove = "a".repeat(29);

        assertThat(FdrCausale.toPersisted(ventinove)).isSameAs(ventinove);
        assertThat(FdrCausale.isTruncated(ventinove)).isFalse();
    }

    /** Il limite e' inclusivo: 30 caratteri entrano in VARCHAR(30). */
    @Test
    void esattamenteTrentaNonVieneTroncato() {
        String trenta = "a".repeat(30);

        assertThat(FdrCausale.toPersisted(trenta)).isSameAs(trenta);
        assertThat(FdrCausale.isTruncated(trenta)).isFalse();
    }

    @Test
    void trentunoVieneTagliatoATrenta() {
        String trentuno = "a".repeat(31);

        assertThat(FdrCausale.toPersisted(trentuno)).hasSize(30).isEqualTo("a".repeat(30));
        assertThat(FdrCausale.isTruncated(trentuno)).isTrue();
    }

    /** La forma reale: 35 caratteri, la lunghezza massima osservata nei file di produzione. */
    @Test
    void laCausaleRealeDaTrentacinqueVienePortataATrenta() {
        String reale = "36042201203713264803200032001234567";

        assertThat(reale).hasSize(35);
        assertThat(FdrCausale.toPersisted(reale)).isEqualTo("360422012037132648032000320012");
        assertThat(FdrCausale.sourceLength(reale)).isEqualTo(35);
    }

    /**
     * Il taglio e' sui code point, non sulle unita' UTF-16, perche' e' cosi' che PostgreSQL conta
     * {@code varchar(30)}. Qui ci sono 29 lettere piu' un emoji: 30 code point, 31 unita' UTF-16.
     * Un clamp sulle unita' lo scarterebbe, pur essendo un valore che la colonna accetta.
     */
    @Test
    void trentaCodePointConUnCarattereFuoriBmpNonVengonoTroncati() {
        String emoji = "😀";
        String valore = "a".repeat(29) + emoji;

        assertThat(valore).hasSize(31);
        assertThat(FdrCausale.sourceLength(valore)).isEqualTo(30);
        assertThat(FdrCausale.toPersisted(valore)).isEqualTo(valore);
    }

    /**
     * E quando il taglio cade in mezzo a una coppia surrogata, l'emoji se ne va intero. Spezzarla
     * lascerebbe un surrogate isolato, che PostgreSQL rifiuta con "invalid byte sequence for
     * encoding UTF8": il troncamento autorizzato diventerebbe la perdita di riga che esiste per
     * evitare.
     */
    @Test
    void ilTaglioNonSpezzaMaiUnaCoppiaSurrogata() {
        String emoji = "😀";
        String valore = "a".repeat(29) + emoji + emoji;

        String persisted = FdrCausale.toPersisted(valore);

        assertThat(persisted).isEqualTo("a".repeat(29) + emoji);
        assertThat(persisted.codePointCount(0, persisted.length())).isEqualTo(30);
        assertThat(Character.isHighSurrogate(persisted.charAt(persisted.length() - 1))).isFalse();
    }

    @Test
    void ilValoreAssenteRestaAssente() {
        assertThat(FdrCausale.toPersisted(null)).isNull();
        assertThat(FdrCausale.isTruncated(null)).isFalse();
        assertThat(FdrCausale.sourceLength(null)).isZero();
    }

    @Test
    void ilValoreVuotoResta() {
        assertThat(FdrCausale.toPersisted("")).isEmpty();
    }

    /** Nessuna normalizzazione, nessun trim, nessun cambio di case: solo la lunghezza cambia. */
    @Test
    void nientAltroVieneModificato() {
        String valore = "  Causale Mista 123  ";

        assertThat(FdrCausale.toPersisted(valore)).isEqualTo(valore);
    }
}
