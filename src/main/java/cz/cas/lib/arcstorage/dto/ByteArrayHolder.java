package cz.cas.lib.arcstorage.dto;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Used for XMLs
 */
public class ByteArrayHolder implements TmpSourceHolder {
    private byte[] file;

    public ByteArrayHolder(byte[] file) {
        this.file = file;
    }

    /**
     * garbage collector will do this
     */
    @Override
    public void freeSpace() {

    }

    @Override
    public InputStream createInputStream() {
        return new ByteArrayInputStream(file);
    }
}
