package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.csv.CsvLineWriter;
import it.pagopa.cruscotto.ingestion.massivesearch.execution.StepMetrics;
import it.pagopa.cruscotto.ingestion.massivesearch.naming.MassiveSearchArtifactNaming;
import it.pagopa.cruscotto.ingestion.massivesearch.report.ReportQueryExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates the Perimeter CSV ({@code NAV;EC}) for a FILTER search instance.
 *
 * <p>Flow: reuse the already-associated CSV on re-execution; otherwise read {@code filter_json},
 * build the dynamic SERT query, stream the distinct pairs into the configured storage and register
 * the file in {@code search_perimeter_file}. Structured logging carries {@code instanceId} and
 * {@code executionId} through the {@code PERIMETER_*} phases.</p>
 */
@Slf4j
@Service
public class PerimeterCsvGenerator {

    /**
     * Header del CSV perimetro generato: template NAV + idDominio -> {@code NAV;EC} (spec ricerca
     * massiva). L'ordine e' NAV poi EC; il separatore e' configurabile. Il lettore
     * ({@code CsvTemplateDetector}) e' comunque case-insensitive e ordine-indipendente.
     */
    private static final List<String> PERIMETER_HEADER = List.of("NAV", "EC");

    private final MassiveSearchProperties properties;
    private final ReportQueryExecutor queryExecutor;
    private final PerimeterQueryBuilder queryBuilder;
    private final CsvLineWriter csvLineWriter;
    private final PerimeterFileRepository repository;
    private final MassiveSearchArtifactNaming naming;
    private final ObjectMapper objectMapper;

    public PerimeterCsvGenerator(
        MassiveSearchProperties properties,
        ReportQueryExecutor queryExecutor,
        PerimeterQueryBuilder queryBuilder,
        CsvLineWriter csvLineWriter,
        PerimeterFileRepository repository,
        MassiveSearchArtifactNaming naming,
        ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.queryExecutor = queryExecutor;
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
                return new PerimeterGenerationResult(reused, true, StepMetrics.create()
                    .with("reused", true)
                    .with("rows", reused.rowsCount())
                    .with("template", reused.template()));
            }

            String filterJson = repository.readFilterJson(instanceId)
                .orElseThrow(() -> new PerimeterGenerationException(
                    "No filter definition (search_filter.filter_json) found for instance " + instanceId));
            PerimeterFilter filter = parseFilter(instanceId, filterJson);

            PerimeterQuery query = queryBuilder.build(filter);
            log.info("phase=PERIMETER_QUERY_BUILT instanceId={} executionId={}", instanceId, executionId);

            String fileName = naming.perimeterFileName(instanceId);

            // The perimeter (NAV;EC header + rows) is generated fully in memory and stored inline in the
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
            long queryStartedNanos = System.nanoTime();
            jdbcQueryPerimeter(query, maxRows, rows, buffer);
            long queryMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - queryStartedNanos);
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
            log.info("phase=PERIMETER_COMPLETED reused=false instanceId={} executionId={} shape={} fileName={} rows={} queryMs={}",
                instanceId, executionId, query.shape(), metadata.fileName(), metadata.rowsCount(), queryMs);
            return new PerimeterGenerationResult(metadata, false, StepMetrics.create()
                .with("reused", false)
                // shape spiega da sola un perimetro piu' grande o piu' lento del previsto: UNION
                // aggiunge il ramo sulle posizioni senza tentativi, POSITION salta la join sui token.
                .with("shape", query.shape())
                .with("template", metadata.template())
                .with("rows", metadata.rowsCount())
                .with("chars", content.length())
                .with("maxRows", maxRows)
                .with("queryMs", queryMs));
        } catch (PerimeterGenerationException e) {
            log.error("phase=PERIMETER_FAILED instanceId={} executionId={} reason={}", instanceId, executionId, e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("phase=PERIMETER_FAILED instanceId={} executionId={} reason={}", instanceId, executionId, e.getMessage(), e);
            throw new PerimeterGenerationException("Perimeter generation failed for instance " + instanceId, e);
        }
    }

    /**
     * Esegue la query di perimetro in streaming reale.
     *
     * <p>Usa lo stesso esecutore dei report ({@link ReportQueryExecutor}): fetchSize + transazione
     * read-only per il cursore server-side, e {@code statement_timeout} lato server. Il
     * {@code jdbc.query} precedente girava senza tetto server-side e con il fetchSize di default,
     * esponendo l'unica query non batchata dell'intero flusso al solo {@code socketTimeout} del
     * client — che lascia la query orfana e distrugge la connessione.</p>
     */
    private void jdbcQueryPerimeter(PerimeterQuery query, int maxRows, AtomicLong rows, StringWriter buffer) {
        queryExecutor.stream(query.sql(), query.params(), rs -> {
            if (maxRows > 0 && rows.incrementAndGet() > maxRows) {
                throw new PerimeterGenerationException(rowLimitMessage(maxRows));
            }
            try {
                // Ordine header NAV;EC: prima il NAV, poi l'idDominio/EC (colonna "pa" della query).
                csvLineWriter.writeLine(buffer, Arrays.asList(rs.getString("nav"), rs.getString("pa")));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
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
