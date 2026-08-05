package it.pagopa.cruscotto.ingestion.massivesearch.perimeter;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Metadata of a {@code search_perimeter_file} row.
 */
public record PerimeterFileMetadata(
    UUID id,
    UUID instanceId,
    UUID executionId,
    String source,
    String template,
    String fileName,
    String filePath,
    long rowsCount,
    String validationStatus,
    OffsetDateTime createdAt
) {}
