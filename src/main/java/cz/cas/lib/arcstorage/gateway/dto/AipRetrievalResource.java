package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

public class AipRetrievalResource extends StorageSessionHolder {

    @Setter
    @Getter
    private InputStream sip;
    @Getter
    private Map<Integer, InputStream> xmls = new TreeMap<>();

    public AipRetrievalResource(Closeable connection) {
        super(connection);
    }

    public void addXml(int version, InputStream xmlInputStream) {
        xmls.put(version, xmlInputStream);
    }
}
