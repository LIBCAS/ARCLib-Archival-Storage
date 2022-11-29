package cz.cas.lib.arcstorage.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class AipRetrievalResource extends StorageSessionHolder {

    /**
     * unique ID for every retrieved AIP, used as a name for tmp file for now
     */
    @Getter
    public String id = UUID.randomUUID().toString();

    /**
     * SIP input stream
     */
    @Setter
    @Getter
    private InputStream sip;
    /**
     * map of XML input streams with XML version as a key
     */
    @Getter
    private Map<Integer, InputStream> xmls = new TreeMap<>();

    public AipRetrievalResource(Closeable connection) {
        super(connection);
    }

    public void addXml(int version, InputStream xmlInputStream) {
        xmls.put(version, xmlInputStream);
    }

    @Override
    public void close() throws IOException {
        super.close();
        sip.close();
        for (InputStream x : xmls.values()) {
            x.close();
        }
    }
}
