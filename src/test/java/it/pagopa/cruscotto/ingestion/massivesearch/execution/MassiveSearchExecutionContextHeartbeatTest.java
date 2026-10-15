package it.pagopa.cruscotto.ingestion.massivesearch.execution;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Il contratto del battito di presenza portato dal contesto.
 *
 * <p>Serve a rendere la soglia del recoverer una misura di <em>vita</em> e non una scadenza fissa
 * sull'istante di avvio: prima un report lungo ma sano superava {@code running-timeout-minutes}
 * mentre stava ancora lavorando e veniva marcato FAILED, lasciando una riga terminale e
 * un'esecuzione viva sul thread.</p>
 */
class MassiveSearchExecutionContextHeartbeatTest {

    private static MassiveSearchExecutionContext context() {
        return new MassiveSearchExecutionContext(UUID.randomUUID(), UUID.randomUUID(), "FILTER", false);
    }

    /** Senza nessun collegamento il battito non deve far nulla: i test e i percorsi non collegati. */
    @Test
    void senzaCollegamentoIlBattitoNonFaNulla() {
        assertDoesNotThrow(() -> context().heartbeat());
    }

    /** Un collegamento azzerato a mano non deve far saltare il report che lo invoca. */
    @Test
    void unCollegamentoNulloNonFaSaltareIlChiamante() {
        MassiveSearchExecutionContext context = context();
        context.setProgressHeartbeat(null);

        assertDoesNotThrow(context::heartbeat);
    }

    /** Il primo battito passa sempre: altrimenti un'esecuzione breve non ne scriverebbe nessuno. */
    @Test
    void ilPrimoBattitoVieneSempreScritto() {
        AtomicInteger beats = new AtomicInteger();
        MassiveSearchExecutionContext context = context();
        context.setProgressHeartbeat(beats::incrementAndGet);

        context.heartbeat();

        assertEquals(1, beats.get());
    }

    /**
     * I battiti sono strozzati a tempo. I chiamanti battono a ogni unita' di lavoro e non devono
     * sapere quanto spesso convenga scrivere: senza la strozzatura, un report con batch piccoli
     * produrrebbe un UPDATE ogni frazione di secondo per ore.
     */
    @Test
    void iBattitiRavvicinatiNonProduconoUnaScrittura() {
        AtomicInteger beats = new AtomicInteger();
        MassiveSearchExecutionContext context = context();
        context.setProgressHeartbeat(beats::incrementAndGet);

        for (int i = 0; i < 10_000; i++) {
            context.heartbeat();
        }

        assertEquals(1, beats.get(), "la strozzatura a tempo non ha retto: un UPDATE per batch");
    }

    /**
     * Un battito fallito non deve mai interrompere il lavoro che sta misurando.
     *
     * <p>E' diagnostica: un errore nello scriverlo — connessione persa, lock — costerebbe l'intero
     * report. Al massimo si perde un battito e, se il problema persiste, l'esecuzione viene
     * recuperata, che a quel punto e' il comportamento corretto.</p>
     */
    @Test
    void unBattitoFallitoNonInterrompeIlReport() {
        MassiveSearchExecutionContext context = context();
        context.setProgressHeartbeat(() -> {
            throw new IllegalStateException("connessione persa");
        });

        assertDoesNotThrow(context::heartbeat);
    }
}
