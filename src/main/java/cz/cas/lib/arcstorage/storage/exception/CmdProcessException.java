package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;

import java.util.Arrays;
import java.util.List;

/**
 * thrown when some command line process performed on the remote storage fails
 */
public class CmdProcessException extends StorageException {

    public CmdProcessException(String cmd, List<String> lines, Storage storage) {
        super(
                "command: " + cmd + " output: " + Arrays.toString(lines.toArray()), storage
        );
    }
}