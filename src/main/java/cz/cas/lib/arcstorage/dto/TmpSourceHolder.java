package cz.cas.lib.arcstorage.dto;

import java.io.InputStream;

public interface TmpSourceHolder {

    InputStream createInputStream();

    /**
     * free space, e.g. deletes tmp file
     */
    void freeSpace();
}
