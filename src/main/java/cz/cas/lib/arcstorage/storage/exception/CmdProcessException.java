package cz.cas.lib.arcstorage.storage.exception;

import java.util.Arrays;
import java.util.List;

public class CmdProcessException extends StorageException {

    public CmdProcessException(String cmd, List<String> lines) {
        super(
                "command: " + cmd + " output: " + Arrays.toString(lines.toArray())
        );
    }
}