package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.io.InputStream;

public class ObjectRetrievalResource extends StorageSessionHolder {
    @Setter
    @Getter
    private InputStream inputStream;

    public ObjectRetrievalResource(InputStream inputStream, Closeable connection) {
        super(connection);
        this.inputStream = inputStream;
    }
}
