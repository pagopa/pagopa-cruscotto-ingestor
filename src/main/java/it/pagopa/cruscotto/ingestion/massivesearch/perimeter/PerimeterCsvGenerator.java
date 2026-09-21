package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvLineWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.naming.MassiveSearchArtifactNaming;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates the Perimeter CSV ({@code PA,NAV}) for a FILTER search instance.
 *
 * <p>Flow: reuse the already-associated CSV on re-execution; otherwise read {@code filter_json},
 * build the dynamic SERT query, stream the distinct pairs into the configured storage and register
 * the file in {@code search_perimeter_file}. Structured logging carries {@code instanceId} and
 * {@code executionId} through the {@code PERIMETER_*} phases.</p>
 */
@Slf4j
@Service
public class PerimeterCsvGenerator {

    /** Column names of the generated perimeter CSV ({@code PA}, {@code NAV}); the separator is configurable. */
    private static final List<String> PERIMETER_HEADER = List.of("PA", "NAV");

    private final MassiveSearchProperties properties;
    private final NamedParameterJdbcTemplate jdbc;
    private final PerimeterQueryBuilder queryBuilder;
    private final CsvLineWriter csvLineWriter;
    private final PerimeterFileRepository repository;
    private final MassiveSearchArtifactNaming naming;
    private final ObjectMapper objectMapper;

    public PerimeterCsvGenerator(
        MassiveSearchProperties properties,
        NamedParameterJdbcTemplate jdbc,
        PerimeterQueryBuilder queryBuilder,
        CsvLineWriter csvLineWriter,
        PerimeterFileRepository repository,
        MassiveSearchArtifactNaming naming,
        ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.jdbc = jdbc;
        this.queryBuilder = queryBuilder;
        this.csvLineWriter = csvLineWriter;
        this.repository = repository;
        this.naming = naming;
        this.objectMapper = objectMapper;
    }

    /**
     * Generates (or reuses) the perimeter CSV of the given instance.
     *
     * @param instanceId  the FILTER search instance
     * @param executionId the current execution correlation id (may be {@code null})
     * @return the generation outcome (fresh or reused)
     */
    public PerimeterGenerationResult generate(UUID instanceId, UUID executionId) {
        log.info("phase=PERIMETER_START instanceId={} executionId={}", instanceId, executionId);
        try {
            Optional<PerimeterFileMetadata> existing = repository.findLatestGenerated(instanceId);
            if (existing.isPresent()) {
                PerimeterFileMetadata reused = existing.orElseThrow();
                ensureWithinRowLimit(reused.rowsCount());
                log.info("phase=PERIMETER_COMPLETED reused=true instanceId={} executionId={} fileName={} rows={}",
                    instanceId, executionId, reused.fileName(), reused.rowsCount());
                return new PerimeterGenerationResult(reused, true);
            }

            String filterJson = repository.readFilterJson(instanceId)
                .orElseThrow(() -> new PerimeterGenerationException(
                    "No filter definition (search_filter.filter_json) found for instance " + instanceId));
            PerimeterFilter filter = parseFilter(instanceId, filterJson);

            PerimeterQuery query = queryBuilder.build(filter);
            log.info("phase=PERIMETER_QUERY_BUILT instanceId={} executionId={}", instanceId, executionId);

            String fileName = naming.perimeterFileName(instanceId);

            // The perimeter (PA,NAV header + rows) is generated fully in memory and stored inline in the
            // DB. It is capped at massive-search.csv.max-rows: exceeding it fails the execution instead
            // of materializing an oversized CSV (and running an unbounded report), with a clear message.
            int maxRows = properties.getCsv().getMaxRows();
            StringWriter buffer = new StringWriter();
            AtomicLong rows = new AtomicLong();
            try {
                csvLineWriter.writeLine(buffer, PERIMETER_HEADER);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            jdbc.query(query.sql(), query.params(), rs -> {
                if (maxRows > 0 && rows.incrementAndGet() > maxRows) {
                    throw new PerimeterGenerationException(rowLimitMessage(maxRows));
                }
                try {
                    csvLineWriter.writeLine(buffer, Arrays.asList(rs.getString("pa"), rs.getString("nav")));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            String content = buffer.toString();

            PerimeterFileMetadata metadata = repository.insertGenerated(
                instanceId,
                executionId,
                properties.getPerimeter().getGeneratedTemplate(),
                fileName,
                content,
                rows.get());

            log.info("phase=PERIMETER_PERSISTED instanceId={} executionId={} storage=db rows={}",
                instanceId, executionId, metadata.rowsCount());
            log.info("phase=PERIMETER_COMPLETED reused=false instanceId={} executionId={} fileName={} rows={}",
                instanceId, executionId, metadata.fileName(), metadata.rowsCount());
            return new PerimeterGenerationResult(metadata, false);
        } catch (PerimeterGenerationException e) {
            log.error("phase=PERIMETER_FAILED instanceId={} executionId={} reason={}", instanceId, executionId, e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("phase=PERIMETER_FAILED instanceId={} executionId={} reason={}", instanceId, executionId, e.getMessage(), e);
            throw new PerimeterGenerationException("Perimeter generation failed for instance " + instanceId, e);
        }
    }

    /** Fails a reused perimeter that already exceeds the configured row cap (e.g. generated before the cap). */
    private void ensureWithinRowLimit(long rowsCount) {
        int maxRows = properties.getCsv().getMaxRows();
        if (maxRows > 0 && rowsCount > maxRows) {
            throw new PerimeterGenerationException(
                "Perimeter has " + rowsCount + " rows, exceeding the maximum of " + maxRows
                    + "; narrow the search filters (or upload a smaller CSV).");
        }
    }

    private static String rowLimitMessage(int maxRows) {
        return "Perimeter exceeds the maximum of " + maxRows
            + " rows; narrow the search filters (or upload a smaller CSV).";
    }

    private PerimeterFilter parseFilter(UUID instanceId, String filterJson) {
        try {
            return objectMapper.readValue(filterJson, PerimeterFilter.class);
        } catch (IOException e) {
            throw new PerimeterGenerationException("Invalid filter_json for instance " + instanceId, e);
        }
    }
}
