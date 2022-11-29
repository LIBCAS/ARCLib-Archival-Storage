package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public class ObjectRetrievalResource extends StorageSessionHolder {

    /**
     * UNIQUE ID for every retrieved object, used as a name for tmp file for now
     * has nothing to do with storage or database id of the object
     */
    @Getter
    private String id = UUID.randomUUID().toString();

    @Setter
    @Getter
    private InputStream inputStream;

    public ObjectRetrievalResource(InputStream inputStream, Closeable connection) {
        super(connection);
        this.inputStream = inputStream;
    }

    @Override
    public void close() throws IOException {
        super.close();
        inputStream.close();
    }
}
