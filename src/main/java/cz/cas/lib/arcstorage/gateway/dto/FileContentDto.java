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

/**
 * General DTO for file content.
 */
@Getter
@Setter
public class FileContentDto {

    /**
     * Input stream of a file. Should be closed via {@link #freeSources()}.
     */
    private InputStream inputStream;

    /**
     * Channels which were used to obtain the file (ssh connection etc.). These should remain open until the file stream is read.
     * Developer is responsible for closing channels via {@link #freeSources()} once the content is read or if exception occurs.
     */
    private List<Closeable> channels = new ArrayList<>();

    public FileContentDto(InputStream inputStream, Closeable... channels) {
        this(inputStream, Arrays.asList(channels));
    }

    public FileContentDto(InputStream inputStream, List<Closeable> channels) {
        this.inputStream = inputStream;
        if (channels != null)
            this.channels = channels;
    }

    /**
     * Closes inputstream and all channels (connections).
     */
    public void freeSources() {
        if (channels.isEmpty())
            return;
        //developer usually calls this right after data are read but it should wait a while because used technology can
        // use some internal after-transfer messaging which would be broken by immediate connection closing
        try {
            Thread.sleep(1000);
            inputStream.close();
            for (Closeable closeable : channels) {
                closeable.close();
            }
        } catch (IOException | InterruptedException e) {
            throw new GeneralException("can't close file channels", e);
        }
    }
}
