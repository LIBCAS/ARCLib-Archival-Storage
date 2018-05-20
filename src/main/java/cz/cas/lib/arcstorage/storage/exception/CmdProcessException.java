package cz.cas.lib.arcstorage.storage.exception;

import java.util.Arrays;
import java.util.List;

/**
 * thrown when some command line process performed on the remote storage fails
 */
public class CmdProcessException extends StorageException {

    public CmdProcessException(String cmd, List<String> lines) {
        super(
                "command: " + cmd + " output: " + Arrays.toString(lines.toArray())
        );
    }
}