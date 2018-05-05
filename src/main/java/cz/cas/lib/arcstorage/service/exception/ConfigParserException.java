package cz.cas.lib.arcstorage.service.exception;

import java.util.Arrays;

public class ConfigParserException extends RuntimeException {

    public ConfigParserException(String pathToNode, String nodeValue, Class<? extends Enum> supportedValues) {
        super("path: " + pathToNode + " value: " + nodeValue + " expected values of " + supportedValues.getSimpleName() + " enum: " + Arrays.toString(supportedValues.getEnumConstants()));
    }

    public ConfigParserException(String message) {
        super(message);
    }

    public ConfigParserException(Throwable cause) {
        super(cause);
    }
}
