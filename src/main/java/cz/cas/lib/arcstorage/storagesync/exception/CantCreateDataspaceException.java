package cz.cas.lib.arcstorage.storagesync.exception;

public class CantCreateDataspaceException extends Exception {

    public CantCreateDataspaceException(String dataspace, Exception e) {
        super("Error while creating dataspace: " + dataspace + " reason: " + e);
    }
}
