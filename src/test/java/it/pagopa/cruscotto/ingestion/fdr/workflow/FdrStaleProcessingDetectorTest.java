package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.config.DbSchemaConfig;
import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Distinguere una giornata ancora in lavorazione da una abbandonata.
 *
 * <p>Il punto di questa classe e' che la domanda <em>non</em> si risponde col tempo trascorso. Una
 * soglia da sola — "ferma da piu' di dieci minuti, quindi e' morta" — dichiarerebbe abbandonata una
 * giornata grande ancora viva, e il recupero che ne seguirebbe farebbe girare due worker sulla
 * stessa giornata. La prova la da' l'heartbeat che l'AS-IS gia' scrive, lo stesso su cui il reaper
 * marca {@code ABORTED}.
 */
class FdrStaleProcessingDetectorTest {

    private static final Instant NOW = Instant.parse("2026-11-18T10:00:00Z");

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);

    @Test
    void unEsecuzioneCheBatteRendeLaGiornataViva() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(1L);

        assertThat(detector(new FdrProperties()).isAnyFdrExecutionAlive(NOW)).isTrue();
    }

    @Test
    void senzaAlcunaEsecuzioneCheBatteNonCeNessunoAlLavoro() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(0L);

        assertThat(detector(new FdrProperties()).isAnyFdrExecutionAlive(NOW)).isFalse();
    }

    /**
     * Si guardano le esecuzioni FDR, e solo quelle.
     *
     * <p>Il prefisso {@code FDR_} sull'entity name non e' decorativo: e' il legame fra i job
     * Quartz e questa interrogazione. Togliere il filtro farebbe considerare viva una giornata FDR
     * perche' sta girando l'ingestione ADX, che non c'entra nulla.
     */
    @Test
    void siGuardanoSoloLeEsecuzioniFdrEsoloQuelleAperte() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(0L);

        detector(new FdrProperties()).isAnyFdrExecutionAlive(NOW);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).queryForObject(sql.capture(), eq(Long.class), any());
        assertThat(sql.getValue())
                .contains("INGEST_EXECUTION_LOG")
                .contains("ENTITY_NAME LIKE 'FDR%'")
                .contains("STATUS = 'STARTED'")
                .contains("LAST_HEARTBEAT_AT >");
    }

    /** La soglia e' configurabile e viene usata davvero, non solo dichiarata. */
    @Test
    void laSogliaDelBattitoVieneDallaConfigurazione() {
        FdrProperties properties = new FdrProperties();
        properties.getRecovery().setStaleAfter(Duration.ofMinutes(30));
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(0L);

        detector(properties).isAnyFdrExecutionAlive(NOW);

        verify(jdbcTemplate).queryForObject(anyString(), eq(Long.class),
                eq(Timestamp.from(NOW.minus(Duration.ofMinutes(30)))));
    }

    /**
     * La soglia di default e' quella del reaper dell'AS-IS.
     *
     * <p>Tenerle diverse significherebbe dichiarare morta una giornata la cui esecuzione risulta
     * ancora viva, o il contrario: due componenti che rispondono in modo opposto alla stessa
     * domanda.
     */
    @Test
    void laSogliaDiDefaultEallineataAlReaper() {
        assertThat(new FdrProperties().getRecovery().getStaleAfter())
                .isEqualTo(Duration.ofMinutes(15));
    }

    // ─── isStale ─────────────────────────────────────────────────────────────────

    @Test
    void unaPresaInCaricoRecenteNonEmaiAbbandonata() {
        FdrStaleProcessingDetector detector = detector(new FdrProperties());

        assertThat(detector.isStale(NOW.minus(Duration.ofMinutes(5)), NOW)).isFalse();
    }

    /**
     * Vecchia <strong>e</strong> senza battito: servono entrambe.
     *
     * <p>E' la congiunzione a essere il presidio. Con la sola soglia si recupererebbe una giornata
     * lenta ancora in corso; col solo battito si recupererebbe al primo buco di un heartbeat.
     */
    @Test
    void servonoInsiemeIlTempoTrascorsoELAssenzaDiBattito() {
        Instant old = NOW.minus(Duration.ofHours(2));

        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(1L);
        assertThat(detector(new FdrProperties()).isStale(old, NOW))
                .as("vecchia ma con un'esecuzione viva")
                .isFalse();

        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(0L);
        assertThat(detector(new FdrProperties()).isStale(old, NOW))
                .as("vecchia e senza nessuno al lavoro")
                .isTrue();
    }

    /** Senza istante di presa in carico non c'e' evidenza di nulla, quindi non si recupera. */
    @Test
    void senzaIstanteDiPresaInCaricoNonSiRecupera() {
        assertThat(detector(new FdrProperties()).isStale(null, NOW)).isFalse();
    }

    private FdrStaleProcessingDetector detector(FdrProperties properties) {
        DbSchemaConfig schema = new DbSchemaConfig();
        schema.setSchema("sert_ingestor");
        return new FdrStaleProcessingDetector(jdbcTemplate, schema, properties);
    }
}
