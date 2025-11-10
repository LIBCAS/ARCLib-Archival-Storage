package cz.cas.lib.arcstorage.dto;

import cz.cas.lib.arcstorage.exception.GeneralException;

import java.io.Closeable;
import java.io.IOException;

public abstract class StorageSessionHolder implements Closeable {

    private static final int SAFE_CLOSE_SLEEP_MS = 1000;

    /**
     * Connection which was used to obtain the files during the session (ssh connection etc.).
     */
    private Closeable connection;

    public StorageSessionHolder(Closeable connection) {
        this.connection = connection;
    }

    /**
     * IMPORTANT: When working with remote storage's retrieval methods, be sure to call this in case of any exception.
     * <p>
     * Use this method to close connection which was used to obtain the files during the session (ssh connection etc.).
     * Connection should remain open until all input streams retrieved during the session are read or no longer needed.
     * </p>
     */
    public void close() throws IOException {
        if (connection == null)
            return;
        //developer usually calls this right after data are read but it should wait a while because used technology can
        // use some internal after-transfer messaging which would be broken by immediate connection closing
        try {
            Thread.sleep(SAFE_CLOSE_SLEEP_MS);
            connection.close();
        } catch (IOException | InterruptedException e) {
            throw new GeneralException("can't close connection", e);
        }
    }
}
