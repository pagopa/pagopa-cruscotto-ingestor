package it.pagopa.cruscotto.ingestion.massivesearch.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.Charset;

/**
 * Storage port for the Massive Search bounded context.
 *
 * <p>Abstracts where perimeter, report and result artifacts are physically written so that the
 * business logic never hardcodes a filesystem path or a blob container. The concrete backend is
 * selected via {@code massive-search.storage.type}: {@code local} (filesystem, development default)
 * or {@code blob} (Azure Blob Storage). The database only ever stores the returned path/reference,
 * never the file content.</p>
 *
 * <p>The mechanical primitives ({@link #store}, {@link #storeBinary}, {@link #openForRead}) are
 * backend-specific; the semantic operations ({@code savePerimeterFile}, {@code saveExecutionCsv},
 * {@code saveResultZip}, {@code loadPerimeterFile}, {@code loadResultZip}) are expressed on top of
 * them so both backends share a single implementation of the higher-level vocabulary.</p>
 */
public interface MassiveSearchStorageService {

    // ── Mechanical primitives (backend specific) ──────────────────────────────────────────────

    /**
     * Streams textual content to the given logical relative path and returns the resolved location
     * together with the number of rows produced by the content writer.
     *
     * @param relativePath  logical path relative to the configured base path / container
     * @param charset       charset used to encode the content
     * @param contentWriter callback that streams the content and returns the number of data rows written
     * @return the stored object descriptor (resolved path and row count)
     */
    StoredObject store(String relativePath, Charset charset, ContentWriter contentWriter);

    /**
     * Streams binary content (e.g. the result ZIP) to the given logical relative path.
     *
     * @param relativePath logical path relative to the configured base path / container
     * @param binaryWriter callback that streams the bytes to the provided output stream
     * @return the stored binary descriptor (resolved path and byte size)
     */
    StoredBinary storeBinary(String relativePath, BinaryWriter binaryWriter);

    /**
     * Opens a stream to read back a previously stored object.
     *
     * @param storagePath the storage path as persisted by {@link StoredObject#path()} / {@link StoredBinary#path()}
     * @return an input stream over the stored content (caller closes it)
     */
    InputStream openForRead(String storagePath);

    /**
     * Deletes a previously stored object if it exists. Used to remove the intermediate per-execution
     * report CSVs once they have been assembled into the result ZIP.
     *
     * @param storagePath the storage path as persisted by {@link StoredObject#path()} / {@link StoredBinary#path()}
     */
    void delete(String storagePath);

    // ── Semantic operations (shared, expressed on the primitives) ─────────────────────────────

    /** Saves the perimeter CSV (functional asset kept for the whole life of the instance). */
    default StoredObject savePerimeterFile(String relativePath, Charset charset, ContentWriter contentWriter) {
        return store(relativePath, charset, contentWriter);
    }

    /** Saves an intermediate per-execution report CSV (temporary, deletable after the ZIP). */
    default StoredObject saveExecutionCsv(String relativePath, Charset charset, ContentWriter contentWriter) {
        return store(relativePath, charset, contentWriter);
    }

    /** Saves the official {@code result.zip} of an execution. */
    default StoredBinary saveResultZip(String relativePath, BinaryWriter binaryWriter) {
        return storeBinary(relativePath, binaryWriter);
    }

    /** Loads a previously saved perimeter / input CSV. */
    default InputStream loadPerimeterFile(String storagePath) {
        return openForRead(storagePath);
    }

    /** Loads a previously saved {@code result.zip}. */
    default InputStream loadResultZip(String storagePath) {
        return openForRead(storagePath);
    }

    // ── Callbacks and descriptors ─────────────────────────────────────────────────────────────

    /** Streaming textual content producer; returns the number of data rows written (header excluded). */
    @FunctionalInterface
    interface ContentWriter {
        long writeTo(Writer writer) throws IOException;
    }

    /** Streaming binary content producer. */
    @FunctionalInterface
    interface BinaryWriter {
        void writeTo(OutputStream outputStream) throws IOException;
    }

    /** Descriptor of a stored textual object. */
    record StoredObject(String path, long rows) {}

    /** Descriptor of a stored binary object. */
    record StoredBinary(String path, long sizeBytes) {}
}
