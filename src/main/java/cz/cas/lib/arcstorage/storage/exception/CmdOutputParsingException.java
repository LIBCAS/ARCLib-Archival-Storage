package cz.cas.lib.arcstorage.storage.exception;

import cz.cas.lib.arcstorage.domain.entity.Storage;

import java.util.Arrays;
import java.util.List;

/**
 * thrown when error occurs during parsing of result of some command line process performed on the remote storage
 */
public class CmdOutputParsingException extends StorageException {
    public CmdOutputParsingException(String cmd, List<String> lines, Storage storage) {
        super(
                "command: " + cmd + " output: " + Arrays.toString(lines.toArray()), storage
        );
    }
}
