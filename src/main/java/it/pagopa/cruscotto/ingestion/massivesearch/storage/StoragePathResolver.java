package it.pagopa.cruscotto.ingestion.massivesearch.storage;

import it.pagopa.cruscotto.ingestion.massivesearch.config.MassiveSearchProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Resolves logical Massive Search relative paths to concrete filesystem locations for the local
 * storage backend. Centralized so the storage service and the ZIP assembler compute paths
 * identically. All segments (root, container, base path) come from configuration; nothing is hardcoded.
 */
@Component
public class StoragePathResolver {

    private final MassiveSearchProperties properties;

    public StoragePathResolver(MassiveSearchProperties properties) {
        this.properties = properties;
    }

    /** Resolves {@code <localRootDir|tmp>/<container>/<basePath>/<relativePath>} normalized. */
    public Path resolve(String relativePath) {
        MassiveSearchProperties.Storage storage = properties.getStorage();
        String root = StringUtils.hasText(storage.getLocalRootDir())
            ? storage.getLocalRootDir()
            : System.getProperty("java.io.tmpdir");

        List<String> segments = new ArrayList<>();
        addSegment(segments, storage.getContainer());
        addSegment(segments, storage.getBasePath());
        addSegment(segments, relativePath);

        Path target = Paths.get(root);
        for (String segment : segments) {
            target = target.resolve(segment);
        }
        return target.normalize();
    }

    private void addSegment(List<String> segments, String value) {
        if (!StringUtils.hasText(value)) {
            return;
        }
        for (String part : value.split("[/\\\\]")) {
            if (StringUtils.hasText(part)) {
                segments.add(part);
            }
        }
    }
}
