package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.naming.MassiveSearchArtifactNaming;
import it.pagopa.cruscotto.ingestion.massivesearch.storage.MassiveSearchStorageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
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

    private final MassiveSearchProperties properties;
    private final NamedParameterJdbcTemplate jdbc;
    private final PerimeterQueryBuilder queryBuilder;
    private final PerimeterCsvWriter csvWriter;
    private final PerimeterFileRepository repository;
    private final MassiveSearchStorageService storage;
    private final MassiveSearchArtifactNaming naming;
    private final ObjectMapper objectMapper;

    public PerimeterCsvGenerator(
        MassiveSearchProperties properties,
        NamedParameterJdbcTemplate jdbc,
        PerimeterQueryBuilder queryBuilder,
        PerimeterCsvWriter csvWriter,
        PerimeterFileRepository repository,
        MassiveSearchStorageService storage,
        MassiveSearchArtifactNaming naming,
        ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.jdbc = jdbc;
        this.queryBuilder = queryBuilder;
        this.csvWriter = csvWriter;
        this.repository = repository;
        this.storage = storage;
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
                log.info("phase=PERIMETER_COMPLETED reused=true instanceId={} executionId={} filePath={} rows={}",
                    instanceId, executionId, existing.get().filePath(), existing.get().rowsCount());
                return new PerimeterGenerationResult(existing.get(), true);
            }

            String filterJson = repository.readFilterJson(instanceId)
                .orElseThrow(() -> new PerimeterGenerationException(
                    "No filter definition (search_filter.filter_json) found for instance " + instanceId));
            PerimeterFilter filter = parseFilter(instanceId, filterJson);

            PerimeterQuery query = queryBuilder.build(filter);
            log.info("phase=PERIMETER_QUERY_BUILT instanceId={} executionId={}", instanceId, executionId);

            String fileName = naming.perimeterFileName(instanceId);
            String relativePath = properties.getStorage().perimeterObjectPath(instanceId, fileName);
            Charset charset = properties.getCsv().getCharset();

            MassiveSearchStorageService.StoredObject stored = storage.savePerimeterFile(relativePath, charset, writer -> {
                csvWriter.writeHeader(writer);
                AtomicLong rows = new AtomicLong();
                jdbc.query(query.sql(), query.params(), rs -> {
                    try {
                        csvWriter.writeRow(writer, rs.getString("pa"), rs.getString("nav"));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    rows.incrementAndGet();
                });
                return rows.get();
            });

            PerimeterFileMetadata metadata = repository.insertGenerated(
                instanceId,
                executionId,
                properties.getPerimeter().getGeneratedTemplate(),
                fileName,
                stored.path(),
                stored.rows());

            log.info("phase=PERIMETER_PERSISTED instanceId={} executionId={} blobPath={} rows={}",
                instanceId, executionId, metadata.filePath(), metadata.rowsCount());
            log.info("phase=PERIMETER_COMPLETED reused=false instanceId={} executionId={} filePath={} rows={}",
                instanceId, executionId, metadata.filePath(), metadata.rowsCount());
            return new PerimeterGenerationResult(metadata, false);
        } catch (PerimeterGenerationException e) {
            log.error("phase=PERIMETER_FAILED instanceId={} executionId={} reason={}", instanceId, executionId, e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("phase=PERIMETER_FAILED instanceId={} executionId={} reason={}", instanceId, executionId, e.getMessage(), e);
            throw new PerimeterGenerationException("Perimeter generation failed for instance " + instanceId, e);
        }
    }

    private PerimeterFilter parseFilter(UUID instanceId, String filterJson) {
        try {
            return objectMapper.readValue(filterJson, PerimeterFilter.class);
        } catch (IOException e) {
            throw new PerimeterGenerationException("Invalid filter_json for instance " + instanceId, e);
        }
    }
}
