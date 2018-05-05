package cz.cas.lib.arcstorage.gateway.dto;

import cz.cas.lib.arcstorage.exception.GeneralException;

import java.io.Closeable;
import java.io.IOException;

public abstract class StorageSessionHolder implements Closeable {

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
    public void close() {
        if (connection == null)
            return;
        //developer usually calls this right after data are read but it should wait a while because used technology can
        // use some internal after-transfer messaging which would be broken by immediate connection closing
        try {
            Thread.sleep(1000);
            connection.close();
        } catch (IOException | InterruptedException e) {
            throw new GeneralException("can't close connection", e);
        }
    }
}
