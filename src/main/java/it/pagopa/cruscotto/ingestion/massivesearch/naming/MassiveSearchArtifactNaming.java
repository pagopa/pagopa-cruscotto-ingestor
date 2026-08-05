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

    /** Name of the downloadable result ZIP for the given execution. */
    public String resultZipFileName(UUID executionId) {
        Naming naming = properties.getNaming();
        return build(naming.getZipPrefix(), executionId, naming.getZipExtension());
    }

    /** Name of the generated perimeter CSV for the given instance. */
    public String perimeterFileName(UUID instanceId) {
        Naming naming = properties.getNaming();
        return build(naming.getPerimeterPrefix(), instanceId, naming.getPerimeterExtension());
    }

    private String build(String prefix, UUID id, String extension) {
        Naming naming = properties.getNaming();
        String timestamp = LocalDateTime
            .now(ZoneId.of(naming.getTimestampZone()))
            .format(DateTimeFormatter.ofPattern(naming.getTimestampPattern()));
        return prefix + naming.getSeparator() + shortId(id, naming.getShortIdLength())
            + naming.getSeparator() + timestamp + extension;
    }

    private String shortId(UUID id, int length) {
        String hex = id.toString().replace("-", "");
        int bounded = Math.min(Math.max(length, 1), hex.length());
        return hex.substring(0, bounded);
    }
}
