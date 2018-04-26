package cz.cas.lib.arcstorage.storage.exception;

import java.util.Arrays;
import java.util.List;

public class CmdOutputParsingException extends StorageException {
    public CmdOutputParsingException(String cmd, List<String> lines) {
        super(
                "command: " + cmd + " output: " + Arrays.toString(lines.toArray())
        );
    }
}
