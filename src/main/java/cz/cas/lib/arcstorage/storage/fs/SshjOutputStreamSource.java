package cz.cas.lib.arcstorage.storage.fs;

import net.schmizz.sshj.xfer.InMemoryDestFile;

import java.io.IOException;
import java.io.OutputStream;

public class SshjOutputStreamSource extends InMemoryDestFile {
    private final OutputStream outputStream;

    public SshjOutputStreamSource(OutputStream os) {
        this.outputStream = os;
    }


    public OutputStream getOutputStream() throws IOException {
        return outputStream;
    }

    @Override
    public long getLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream getOutputStream(boolean append) throws IOException {
        return outputStream;
    }
}
