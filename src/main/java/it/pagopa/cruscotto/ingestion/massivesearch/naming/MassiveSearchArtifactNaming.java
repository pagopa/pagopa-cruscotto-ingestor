package it.pagopa.cruscotto.ingestion.massivesearch.naming;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties.Naming;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Single source of truth for the deliverable artifact names of the Massive Search feature.
 *
 * <p>Builds names following the convention {@code <prefix><sep><shortId><sep><timestamp><ext>} (e.g.
 * {@code ricerca-massiva__a1b2c3d4__20260804-153500.zip}), where the short id are the leading hex
 * characters of the correlation UUID and the timestamp is rendered from configuration. All tokens are
 * resolved from {@link MassiveSearchProperties.Naming}; nothing is hardcoded.</p>
 */
@Component
public class MassiveSearchArtifactNaming {

    private final MassiveSearchProperties properties;

    public MassiveSearchArtifactNaming(MassiveSearchProperties properties) {
        this.properties = properties;
    }

    /**
     * Timestamp captured once per execution, in the configured zone. Passed to
     * {@link #resultZipFileName(UUID, LocalDateTime)} and {@link #reportFileName(String, UUID, LocalDateTime)}
     * so the ZIP and the report CSVs it contains share the exact same timestamp token.
     */
    public LocalDateTime executionTimestamp() {
        return LocalDateTime.now(ZoneId.of(properties.getNaming().getTimestampZone()));
    }

    /** Name of the downloadable result ZIP for the given execution, at the shared execution timestamp. */
    public String resultZipFileName(UUID executionId, LocalDateTime timestamp) {
        Naming naming = properties.getNaming();
        return build(naming.getZipPrefix(), executionId, naming.getZipExtension(), timestamp);
    }

    /**
     * Name of a report CSV kept inside the result ZIP. Follows the same convention as the ZIP, using the
     * same {@code executionId} (short id) and the same execution timestamp, so the archive and its CSVs
     * are visibly a single set (e.g. {@code ricerca-massiva__a1b2c3d4__20260804-153500.zip} contains
     * {@code posizioni__a1b2c3d4__20260804-153500.csv}).
     */
    public String reportFileName(String prefix, UUID executionId, LocalDateTime timestamp) {
        return build(prefix, executionId, properties.getReports().getExtension(), timestamp);
    }

    /** Name of the generated perimeter CSV for the given instance (own timestamp, distinct artifact). */
    public String perimeterFileName(UUID instanceId) {
        Naming naming = properties.getNaming();
        return build(naming.getPerimeterPrefix(), instanceId, naming.getPerimeterExtension(), executionTimestamp());
    }

    private String build(String prefix, UUID id, String extension, LocalDateTime timestamp) {
        Naming naming = properties.getNaming();
        String ts = timestamp.format(DateTimeFormatter.ofPattern(naming.getTimestampPattern()));
        return prefix + naming.getSeparator() + shortId(id, naming.getShortIdLength())
            + naming.getSeparator() + ts + extension;
    }

    private String shortId(UUID id, int length) {
        String hex = id.toString().replace("-", "");
        int bounded = Math.min(Math.max(length, 1), hex.length());
        return hex.substring(0, bounded);
    }
}
