package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrBlobFile;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.math.BigInteger;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * L'apertura di una sessione di scrittura.
 *
 * <p>Poco codice e una sola garanzia, ma e' quella che impedisce il danno piu' silenzioso di tutti:
 * la giornata senza partizione esplicita viene fermata <strong>prima</strong> che si scriva, non
 * dopo. Senza, le righe finirebbero nella DEFAULT senza alcun errore e una reingestion della stessa
 * giornata le duplicherebbe invece di sostituirle.
 */
@ExtendWith(MockitoExtension.class)
class FdrRowWriterTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    @Mock
    private JdbcTemplate jdbcTemplate;
    @Mock
    private PlatformTransactionManager transactionManager;
    @Mock
    private FdrPartitionGuard partitionGuard;

    @Test
    void laPartizioneVieneVerificataPrimaDiAprireLaSessione() {
        FdrWriteSession session = writer().openSession(file(), FdrDayFence.NONE);

        verify(partitionGuard).requireExplicitPartition(FdrDataset.FLUSSI_RENDICONTAZIONE, DAY);
        assertThat(session).isNotNull();
        verifyNoInteractions(jdbcTemplate);
    }

    /**
     * Il controllo sta all'apertura e non al primo batch: una giornata fuori orizzonte fallisce
     * prima che il file venga scaricato e analizzato, invece che dopo.
     */
    @Test
    void senzaPartizioneLaSessioneNonSiApreNeppure() {
        doThrow(new FdrPersistenceException(FdrPersistenceException.Kind.MISSING_PARTITION,
                "no partition"))
                .when(partitionGuard).requireExplicitPartition(FdrDataset.IUV_RENDICONTATI, DAY);

        assertThatThrownBy(() -> writer().openSession(
                new FdrWriteContext(FdrDataset.IUV_RENDICONTATI, DAY, "20260915/iuv-1.csv.gz"), FdrDayFence.NONE))
                .isInstanceOf(FdrPersistenceException.class)
                .extracting(e -> ((FdrPersistenceException) e).kind())
                .isEqualTo(FdrPersistenceException.Kind.MISSING_PARTITION);

        verifyNoInteractions(jdbcTemplate);
    }

    /** La sessione riceve il contesto del file: dataset, giornata della cartella, nome del blob. */
    @Test
    void laSessionePortaIlContestoDelFile() {
        FdrWriteResult result = writer().openSession(file(), FdrDayFence.NONE).complete();

        assertThat(result.dataset()).isEqualTo(FdrDataset.FLUSSI_RENDICONTAZIONE);
        assertThat(result.businessDay()).isEqualTo(DAY);
        assertThat(result.blobName())
                .isEqualTo("20260915/FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz");
    }

    private FdrRowWriter writer() {
        FdrProperties properties = new FdrProperties();
        properties.getProcessing().setBatchSize(1000);
        return new FdrRowWriter(jdbcTemplate, transactionManager, partitionGuard,
                new DbSchemaConfig(), properties);
    }

    private static FdrBlobFile file() {
        return new FdrBlobFile("20260915/FLUSSI_RENDICONTAZIONE_20260915-1.csv.gz",
                FdrDataset.FLUSSI_RENDICONTAZIONE, DAY, BigInteger.ONE, -1L, null, null);
    }
}
