package cz.cas.lib.arcstorage.dto;

import java.io.*;

/**
 * used for general objects
 */
public class TmpFileHolder implements TmpSourceHolder {

    private File tmpFile;

    public TmpFileHolder(File tmpFile) {
        this.tmpFile = tmpFile;
    }

    @Override
    public InputStream createInputStream() {
        try {
            return new FileInputStream(tmpFile);
        } catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void freeSpace() {
        tmpFile.delete();
    }
}
