package it.pagopa.cruscotto.ingestion.fdr.persistence;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * La guardia che impedisce una scrittura silenziosa nella DEFAULT.
 *
 * <p>Senza la DEFAULT una giornata fuori orizzonte farebbe fallire l'INSERT e il problema sarebbe
 * evidente. Con la DEFAULT le righe vengono accettate senza errore e finiscono in una partizione
 * non potabile, che cresce senza limite e che la reingestion non svuota: rilanciare quella
 * giornata la duplicherebbe invece di sostituirla.
 */
class FdrPartitionGuardTest {

    private static final LocalDate DAY = LocalDate.of(2026, 9, 15);

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final FdrPartitionGuard guard =
            new FdrPartitionGuard(jdbcTemplate, new DbSchemaConfig());

    @Test
    void conLaPartizioneEsplicitaLaScritturaProsegue() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any(Object[].class)))
                .thenReturn(1L);

        assertThatCode(() -> guard.requireExplicitPartition(FdrDataset.FLUSSI_RENDICONTAZIONE, DAY))
                .doesNotThrowAnyException();
    }

    @Test
    void senzaPartizioneEsplicitaLaScritturaSiFermaPrimaDiScrivere() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any(Object[].class)))
                .thenReturn(0L);

        assertThatThrownBy(() ->
                guard.requireExplicitPartition(FdrDataset.IUV_RENDICONTATI, LocalDate.of(2030, 1, 1)))
                .isInstanceOf(FdrPersistenceException.class)
                .hasMessageContaining("FDR_IUV_RENDICONTATI_20300101")
                .hasMessageContaining("DEFAULT")
                .extracting(e -> ((FdrPersistenceException) e).kind())
                .isEqualTo(FdrPersistenceException.Kind.MISSING_PARTITION);
    }

    /** Una risposta nulla del catalogo non e' un via libera. */
    @Test
    void unaRispostaNullaVieneTrattataComePartizioneAssente() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any(Object[].class)))
                .thenReturn(null);

        assertThatThrownBy(() -> guard.requireExplicitPartition(FdrDataset.FLUSSI_RENDICONTAZIONE, DAY))
                .isInstanceOf(FdrPersistenceException.class);
    }

    /**
     * Il nome e' quello che il changeSet crea con {@code format('%I', ...)}: maiuscolo e con il
     * giorno in {@code yyyyMMdd}. Verificato sul database di dev, dove la partizione del 15/09
     * esiste con questo nome esatto.
     */
    @Test
    void ilNomeDellaPartizioneSegueLaConvenzioneDelChangeSet() {
        assertThat(FdrPartitionGuard.partitionName(FdrDataset.FLUSSI_RENDICONTAZIONE, DAY))
                .isEqualTo("FDR_FLUSSI_RENDICONTAZIONE_20260915");
        assertThat(FdrPartitionGuard.partitionName(FdrDataset.IUV_RENDICONTATI, DAY))
                .isEqualTo("FDR_IUV_RENDICONTATI_20260915");
    }

    /** Il prefisso FDR_ sta sulle tabelle, non sui file: le due cose non vanno allineate. */
    @Test
    void leTabelleHannoIlPrefissoCheIFileNonHanno() {
        assertThat(FdrPartitionGuard.tableName(FdrDataset.FLUSSI_RENDICONTAZIONE))
                .isEqualTo("FDR_FLUSSI_RENDICONTAZIONE");
        assertThat(FdrDataset.FLUSSI_RENDICONTAZIONE.filePrefix())
                .isEqualTo("FLUSSI_RENDICONTAZIONE");
    }
}
