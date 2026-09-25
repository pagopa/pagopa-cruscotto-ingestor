package it.pagopa.cruscotto.ingestion.massivesearch.storage;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Default {@link MassiveSearchStorageService} backed by the local filesystem.
 *
 * <p>Active unless another backend is selected via {@code massive-search.storage.type}. Paths are
 * resolved by {@link StoragePathResolver} from configuration, so nothing is hardcoded. The Azure
 * Blob backend is provided separately by the result persistence step.</p>
 */
@Slf4j
@Service
@ConditionalOnProperty(prefix = "massive-search.storage", name = "type", havingValue = "local", matchIfMissing = true)
public class LocalFileSystemStorageService implements MassiveSearchStorageService {

    private final StoragePathResolver pathResolver;

    public LocalFileSystemStorageService(StoragePathResolver pathResolver) {
        this.pathResolver = pathResolver;
    }

    @Override
    public StoredObject store(String relativePath, Charset charset, ContentWriter contentWriter) {
        Path target = pathResolver.resolve(relativePath);
        try {
            Files.createDirectories(target.getParent());
            try (BufferedWriter writer = Files.newBufferedWriter(target, charset)) {
                long rows = contentWriter.writeTo(writer);
                return new StoredObject(target.toString(), rows);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to store Massive Search file at " + target, e);
        }
    }

    @Override
    public StoredBinary storeBinary(String relativePath, BinaryWriter binaryWriter) {
        Path target = pathResolver.resolve(relativePath);
        try {
            Files.createDirectories(target.getParent());
            try (OutputStream out = Files.newOutputStream(target);
                 CountingOutputStream counting = new CountingOutputStream(out)) {
                binaryWriter.writeTo(counting);
                counting.flush();
                return new StoredBinary(target.toString(), counting.getCount());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to store Massive Search binary at " + target, e);
        }
    }

    @Override
    public InputStream openForRead(String storagePath) {
        Path source = Path.of(storagePath);
        try {
            return Files.newInputStream(source);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read Massive Search file at " + source, e);
        }
    }

    @Override
    public void delete(String storagePath) {
        try {
            Files.deleteIfExists(Path.of(storagePath));
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to delete Massive Search file at " + storagePath, e);
        }
    }
}
