package cz.cas.lib.arcstorage.gateway.dto;

import cz.cas.lib.arcstorage.exception.GeneralException;
import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class FileRef {
    /**
     * shuld be closed once the object usage ends
     */
    private InputStream inputStream;
    /**
     * use {@link #freeSources()} method to close these after transfer
     */
    private List<Closeable> channels = new ArrayList<>();

    public FileRef(InputStream inputStream, Closeable... channels) {
        this(inputStream, Arrays.asList(channels));
    }

    public FileRef(InputStream inputStream, List<Closeable> channels) {
        this.inputStream = inputStream;
        if (channels != null)
            this.channels = channels;
    }

    /**
     * Closes inputstream and all channels (connections) which have to be closed once the object usage ends.
     */
    public void freeSources() {
        if (channels.isEmpty())
            return;
        //developer usually calls this right after data are read but it should wait a while because used technology can
        // use some internal after-transfer messaging which would be broken by immediate connection closing
        try {
            Thread.sleep(1000);
            for (Closeable closeable : channels) {
                inputStream.close();
                closeable.close();
            }
        } catch (IOException | InterruptedException e) {
            throw new GeneralException("can't close file channels", e);
        }
    }
}
