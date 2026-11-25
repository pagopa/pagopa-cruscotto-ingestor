package it.pagopa.cruscotto.ingestion.fdr.workflow;

import it.pagopa.cruscotto.ingestion.fdr.config.FdrProperties;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * L'elaborazione giornaliera: quale giornata, e quando vale la pena rifarla.
 *
 * <p>Due responsabilita' sole, ed e' tutto quello che questa classe deve avere: scegliere la data e
 * decidere se c'e' qualcosa da fare. L'elaborazione vera e' del core e ha i suoi test.
 *
 * <p>L'orologio e' <strong>sempre</strong> iniettato. Un test che chiedesse "ieri" al calendario
 * vero passerebbe oggi e fallirebbe il 1 marzo, e quel genere di fallimento arriva in CI senza
 * alcun cambiamento di codice a cui attribuirlo.
 */
class FdrDailyServiceTest {

    private final FdrDayProcessor dayProcessor = mock(FdrDayProcessor.class);
    private final FdrBatchRepository batchRepository = mock(FdrBatchRepository.class);
    private final FdrStaleProcessingDetector staleDetector = mock(FdrStaleProcessingDetector.class);

    // ─── La giornata di competenza ───────────────────────────────────────────────

    @Test
    void elaboraLaGiornataPrecedenteNonQuellaCorrente() {
        FdrDailyService service = serviceAt("2026-09-16T05:00:00Z", new FdrProperties());

        assertThat(service.targetBusinessDay()).isEqualTo(LocalDate.of(2026, 9, 15));
    }

    /**
     * Il primo del mese e' il caso che una sottrazione sbagliata sopravvive: {@code withDayOfMonth}
     * o un'aritmetica fatta a mano sul giorno darebbero il 1 o il 0 di settembre, non il 31 agosto.
     */
    @Test
    void ilPrimoDelMeseTornaAllUltimoGiornoDelMesePrecedente() {
        FdrDailyService service = serviceAt("2026-09-01T05:00:00Z", new FdrProperties());

        assertThat(service.targetBusinessDay()).isEqualTo(LocalDate.of(2026, 8, 31));
    }

    @Test
    void ilPrimoDellAnnoTornaAlTrentunoDicembrePrecedente() {
        FdrDailyService service = serviceAt("2027-01-01T05:00:00Z", new FdrProperties());

        assertThat(service.targetBusinessDay()).isEqualTo(LocalDate.of(2026, 12, 31));
    }

    /** Il 1 marzo di un anno bisestile: il giorno prima esiste, ed e' il 29. */
    @Test
    void ilPrimoMarzoDiUnAnnoBisestileTornaAlVentinove() {
        FdrDailyService service = serviceAt("2028-03-01T05:00:00Z", new FdrProperties());

        assertThat(service.targetBusinessDay()).isEqualTo(LocalDate.of(2028, 2, 29));
    }

    /**
     * Il fuso conta davvero: a mezzanotte e mezza di Roma a Londra e' ancora il giorno prima, quindi
     * "ieri" e' un giorno diverso a seconda di chi lo chiede. La property esiste per questo.
     */
    @Test
    void laGiornataSiLeggeNelFusoConfigurato() {
        FdrProperties properties = new FdrProperties();
        Clock rome = Clock.fixed(Instant.parse("2026-09-15T22:30:00Z"), ZoneId.of("Europe/Rome"));
        Clock utc = Clock.fixed(Instant.parse("2026-09-15T22:30:00Z"), ZoneOffset.UTC);

        FdrDailyService inRome = new FdrDailyService(dayProcessor, batchRepository, staleDetector,
                properties, rome);
        FdrDailyService inUtc = new FdrDailyService(dayProcessor, batchRepository, staleDetector,
                properties, utc);

        // A Roma sono gia' le 00:30 del 16, quindi ieri e' il 15; in UTC e' ancora il 15 e ieri e'
        // il 14.
        assertThat(inRome.targetBusinessDay()).isEqualTo(LocalDate.of(2026, 9, 15));
        assertThat(inUtc.targetBusinessDay()).isEqualTo(LocalDate.of(2026, 9, 14));
    }

    @Test
    void lOverrideSostituisceLaGiornataCalcolata() {
        FdrProperties properties = new FdrProperties();
        properties.getDaily().setOverrideBusinessDay("2026-09-15");

        FdrDailyService service = serviceAt("2026-11-18T05:00:00Z", properties);

        assertThat(service.targetBusinessDay()).isEqualTo(LocalDate.of(2026, 9, 15));
    }

    /**
     * Un override vuoto o fatto di spazi e' il valore normale in produzione: la variabile d'ambiente
     * esiste e non e' valorizzata. Deve comportarsi come assente, non come una data da parsare.
     */
    @Test
    void unOverrideVuotoNonCambiaNulla() {
        FdrProperties properties = new FdrProperties();
        properties.getDaily().setOverrideBusinessDay("   ");

        FdrDailyService service = serviceAt("2026-09-16T05:00:00Z", properties);

        assertThat(service.targetBusinessDay()).isEqualTo(LocalDate.of(2026, 9, 15));
    }

    // ─── Idempotenza ─────────────────────────────────────────────────────────────

    @Test
    void unaGiornataMaiVistaVieneElaborata() {
        when(batchRepository.findDailyOutcome(any())).thenReturn(Optional.empty());
        when(batchRepository.createProcessing(any(), any(), any(), anyString(), any()))
                .thenReturn(42L);
        when(dayProcessor.process(42L, LocalDate.of(2026, 9, 15)))
                .thenReturn(dayResult(42L, FdrProcessingStatus.COMPLETED));

        Optional<FdrDayResult> result = serviceAt("2026-09-16T05:00:00Z", new FdrProperties())
                .runDaily();

        assertThat(result).isPresent();
        verify(dayProcessor).process(42L, LocalDate.of(2026, 9, 15));
        verify(batchRepository).complete(eq(42L), eq(FdrProcessingStatus.COMPLETED), any());
    }

    /**
     * I quattro stati che fermano il DAILY, ciascuno per una ragione diversa.
     *
     * <p>{@code COMPLETED} e {@code COMPLETED_WITH_WARNING} perche' rielaborare raddoppierebbe le
     * righe — la scrittura e' un INSERT fedele, senza upsert. {@code WAITING_RETRY} perche' il
     * tentativo successivo ha gia' un orario e ripartire adesso lo anticiperebbe, ignorando
     * l'intervallo di un'ora. {@code FAILED} perche' un fallimento terminale vuole una decisione
     * umana, cioe' una REINGESTION.
     */
    @Test
    void unaGiornataGiaChiusaNonVieneRielaborata() {
        for (FdrProcessingStatus settled : new FdrProcessingStatus[] {
                FdrProcessingStatus.COMPLETED, FdrProcessingStatus.COMPLETED_WITH_WARNING,
                FdrProcessingStatus.WAITING_RETRY, FdrProcessingStatus.FAILED}) {
            when(batchRepository.findDailyOutcome(any())).thenReturn(Optional.of(settled));

            Optional<FdrDayResult> result = serviceAt("2026-09-16T05:00:00Z", new FdrProperties())
                    .runDaily();

            assertThat(result).as("stato %s", settled).isEmpty();
        }
        verify(dayProcessor, never()).process(anyLong(), any());
        verify(batchRepository, never()).createProcessing(any(), any(), any(), anyString(), any());
    }

    @Test
    void unaGiornataInLavorazioneConUnEsecuzioneVivaNonVieneToccata() {
        when(batchRepository.findDailyOutcome(any()))
                .thenReturn(Optional.of(FdrProcessingStatus.PROCESSING));
        when(staleDetector.isAnyFdrExecutionAlive(any())).thenReturn(true);

        Optional<FdrDayResult> result = serviceAt("2026-09-16T05:00:00Z", new FdrProperties())
                .runDaily();

        assertThat(result).isEmpty();
        verify(dayProcessor, never()).process(anyLong(), any());
    }

    /**
     * Lo stesso stato, l'esito opposto: senza alcuna esecuzione FDR viva la giornata e' di un pod
     * che non c'e' piu', e va ripresa. E' il motivo per cui il rilevatore esiste — una soglia sul
     * tempo da sola direbbe "abbandonata" anche a una giornata grande ancora in corso.
     */
    @Test
    void unaGiornataInLavorazioneAbbandonataVieneRipresa() {
        when(batchRepository.findDailyOutcome(any()))
                .thenReturn(Optional.of(FdrProcessingStatus.PROCESSING));
        when(staleDetector.isAnyFdrExecutionAlive(any())).thenReturn(false);
        when(batchRepository.createProcessing(any(), any(), any(), anyString(), any()))
                .thenReturn(99L);
        when(dayProcessor.process(anyLong(), any()))
                .thenReturn(dayResult(99L, FdrProcessingStatus.COMPLETED));

        Optional<FdrDayResult> result = serviceAt("2026-09-16T05:00:00Z", new FdrProperties())
                .runDaily();

        assertThat(result).isPresent();
        verify(dayProcessor).process(99L, LocalDate.of(2026, 9, 15));
    }

    /**
     * Il DAILY nasce gia' {@code PROCESSING} e con gli estremi uguali fra loro.
     *
     * <p>Non {@code PENDING}: il command poller raccoglie le richieste in attesa, e un DAILY creato
     * cosi' verrebbe preso anche da lui — due elaborazioni della stessa giornata, una delle quali
     * scritta due volte.
     */
    @Test
    void ilBatchGiornalieroNasceGiaInLavorazioneSullaStessaGiornata() {
        when(batchRepository.findDailyOutcome(any())).thenReturn(Optional.empty());
        when(batchRepository.createProcessing(any(), any(), any(), anyString(), any()))
                .thenReturn(1L);
        when(dayProcessor.process(anyLong(), any()))
                .thenReturn(dayResult(1L, FdrProcessingStatus.COMPLETED));

        serviceAt("2026-09-16T05:00:00Z", new FdrProperties()).runDaily();

        LocalDate day = LocalDate.of(2026, 9, 15);
        verify(batchRepository).createProcessing(eq(FdrWorkflow.DAILY), eq(day), eq(day),
                anyString(), any());
    }

    // ─── Utilita' ────────────────────────────────────────────────────────────────

    private FdrDailyService serviceAt(String instant, FdrProperties properties) {
        return new FdrDailyService(dayProcessor, batchRepository, staleDetector, properties,
                Clock.fixed(Instant.parse(instant), ZoneOffset.UTC));
    }

    private static FdrDayResult dayResult(long batchId, FdrProcessingStatus status) {
        Map<it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset, FdrDatasetResult> datasets =
                new EnumMap<>(it.pagopa.cruscotto.ingestion.fdr.storage.FdrDataset.class);
        return new FdrDayResult(batchId, LocalDate.of(2026, 9, 15), status, datasets, false);
    }
}
