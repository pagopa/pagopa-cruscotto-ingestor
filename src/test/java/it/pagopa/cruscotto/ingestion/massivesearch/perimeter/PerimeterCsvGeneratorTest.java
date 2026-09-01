package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvLineWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.naming.MassiveSearchArtifactNaming;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that the perimeter generator assembles the {@code PA,NAV} CSV through the shared
 * {@link CsvLineWriter} and reuses the existing perimeter on re-execution.
 */
class PerimeterCsvGeneratorTest {

    private NamedParameterJdbcTemplate jdbc;
    private PerimeterQueryBuilder queryBuilder;
    private PerimeterFileRepository repository;
    private MassiveSearchArtifactNaming naming;
    private PerimeterCsvGenerator generator;

    private final UUID instanceId = UUID.randomUUID();
    private final UUID executionId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        jdbc = mock(NamedParameterJdbcTemplate.class);
        queryBuilder = mock(PerimeterQueryBuilder.class);
        repository = mock(PerimeterFileRepository.class);
        naming = mock(MassiveSearchArtifactNaming.class);
        generator = new PerimeterCsvGenerator(
            new MassiveSearchProperties(), jdbc, queryBuilder, new CsvLineWriter(new MassiveSearchProperties()),
            repository, naming, new ObjectMapper());
    }

    private ResultSet pair(String pa, String nav) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString("pa")).thenReturn(pa);
        when(rs.getString("nav")).thenReturn(nav);
        return rs;
    }

    private PerimeterFileMetadata metadata(String content, long rows) {
        return new PerimeterFileMetadata(UUID.randomUUID(), instanceId, executionId, "GENERATED",
            "NAV_PA", "perimetro.csv", null, rows, "NOT_VALIDATED", OffsetDateTime.now(), content);
    }

    @Test
    void generatesPaNavCsvWithHeaderAndOneRowPerPair() throws SQLException {
        when(repository.findLatestGenerated(instanceId)).thenReturn(Optional.empty());
        when(repository.readFilterJson(instanceId)).thenReturn(Optional.of("{}"));
        when(queryBuilder.build(any())).thenReturn(new PerimeterQuery("SELECT ...", new MapSqlParameterSource()));
        when(naming.perimeterFileName(instanceId)).thenReturn("perimetro.csv");

        doAnswer(inv -> {
            RowCallbackHandler handler = inv.getArgument(2);
            handler.processRow(pair("00147990923", "301000000000000001"));
            handler.processRow(pair("00147990923", "301000000000000002"));
            return null;
        }).when(jdbc).query(anyString(), any(MapSqlParameterSource.class), any(RowCallbackHandler.class));

        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> rowsCaptor = ArgumentCaptor.forClass(Long.class);
        when(repository.insertGenerated(any(), any(), anyString(), anyString(), contentCaptor.capture(), rowsCaptor.capture()))
            .thenAnswer(inv -> metadata(inv.getArgument(4), inv.getArgument(5)));

        PerimeterGenerationResult result = generator.generate(instanceId, executionId);

        assertFalse(result.reused());
        assertEquals(2L, rowsCaptor.getValue());
        assertEquals(
            "PA,NAV\r\n00147990923,301000000000000001\r\n00147990923,301000000000000002\r\n",
            contentCaptor.getValue());
    }

    @Test
    void reusesExistingPerimeterWithoutQueryingOrInserting() {
        PerimeterFileMetadata existing = metadata("PA,NAV\r\n", 0);
        when(repository.findLatestGenerated(instanceId)).thenReturn(Optional.of(existing));

        PerimeterGenerationResult result = generator.generate(instanceId, executionId);

        assertTrue(result.reused());
        assertEquals(existing, result.file());
        verify(jdbc, never()).query(anyString(), any(MapSqlParameterSource.class), any(RowCallbackHandler.class));
        verify(repository, never()).insertGenerated(any(), any(), anyString(), anyString(), anyString(), anyLong());
    }
}
