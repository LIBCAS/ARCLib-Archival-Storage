package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.InputStream;

@Getter
@Setter
public class XmlFileRef extends FileRef {
    private int version;

    public XmlFileRef(String id, InputStream stream, Checksum checksum, int version) {
        super(id, stream, checksum);
        this.version = version;
    }

    public XmlFileRef(XmlFileRef xml, InputStream xmlIs) {
        this(xml.getId(), xmlIs, xml.getChecksum(), xml.getVersion());
    }
}
