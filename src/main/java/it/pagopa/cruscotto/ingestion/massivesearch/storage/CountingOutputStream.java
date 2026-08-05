package it.pagopa.cruscotto.ingestion.massivesearch.storage;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link OutputStream} decorator that counts the number of bytes written, so storage backends can
 * report the size of a streamed binary artifact without buffering it in memory.
 */
public class CountingOutputStream extends FilterOutputStream {

    private long count;

    public CountingOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        count += len;
    }

    /** Total number of bytes written so far. */
    public long getCount() {
        return count;
    }
}
