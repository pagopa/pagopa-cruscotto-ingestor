package it.pagopa.cruscotto.ingestion.fdr.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L'identificativo del processo che possiede una giornata.
 *
 * <p>Vale la pena un test perché il valore finisce <strong>in colonna</strong>, su
 * {@code FDR_INGESTION_STATUS.PROCESSING_INSTANCE_ID}, e in produzione non c'è accesso diretto ai
 * log: se fosse vuoto o cambiasse a ogni lettura, una giornata bloccata non sarebbe attribuibile ad
 * alcun processo — che è l'unica cosa per cui quella colonna esiste.
 *
 * <p>Il valore vero dipende dall'ambiente ({@code HOSTNAME} è il nome del pod in Kubernetes) e non
 * è manipolabile da un test senza riscrivere l'ambiente del processo: qui si verificano le due
 * proprietà che devono valere ovunque.
 */
class FdrInstanceIdTest {

    /**
     * Mai vuoto.
     *
     * <p>È la ragione del ripiego su un UUID fuori da Kubernetes: una stringa vuota in colonna si
     * confonderebbe con "non ancora scritto", e chi legge non avrebbe modo di distinguere un
     * claim mai avvenuto da un claim avvenuto su una macchina senza {@code HOSTNAME}.
     */
    @Test
    @DisplayName("l'identificativo del processo non e' mai vuoto")
    void lIdentificativoNonEmaiVuoto() {
        assertThat(FdrInstanceId.get()).isNotBlank();
    }

    /**
     * Stabile per tutta la vita del processo.
     *
     * <p>Se cambiasse a ogni lettura, due righe della stessa giornata potrebbero riceverne due
     * diversi — e la colonna smetterebbe di rispondere a "chi devo andare a guardare". È il motivo
     * per cui è risolto una volta sola in un campo statico e non calcolato a ogni chiamata.
     */
    @Test
    @DisplayName("l'identificativo resta lo stesso per tutta la vita del processo")
    void lIdentificativoEstabile() {
        assertThat(FdrInstanceId.get()).isEqualTo(FdrInstanceId.get());
    }

    /**
     * Non è un identificativo di esecuzione.
     *
     * <p>Il contrario — un UUID nuovo a ogni chiamata — è esattamente cosa <em>non</em> deve
     * essere: quello è il correlation id, che identifica un giro del job e vive solo nei log. Qui
     * serve l'opposto, un valore che sopravviva a tutte le giornate lavorate dallo stesso pod.
     */
    @Test
    @DisplayName("non e' un identificativo nuovo a ogni esecuzione")
    void nonEunIdentificativoPerEsecuzione() {
        String first = FdrInstanceId.get();
        for (int i = 0; i < 100; i++) {
            assertThat(FdrInstanceId.get()).isEqualTo(first);
        }
    }
}
