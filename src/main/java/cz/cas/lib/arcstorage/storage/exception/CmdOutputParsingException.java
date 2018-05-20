package cz.cas.lib.arcstorage.storage.exception;

import java.util.Arrays;
import java.util.List;

/**
 * thrown when error occurs during parsing of result of some command line process performed on the remote storage
 */
public class CmdOutputParsingException extends StorageException {
    public CmdOutputParsingException(String cmd, List<String> lines) {
        super(
                "command: " + cmd + " output: " + Arrays.toString(lines.toArray())
        );
    }
}
