package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.InputStream;

@Getter
@Setter
public class XmlRef extends ArchiveFileRef {
    private int version;

    public XmlRef(String id, FileRef fileRef, Checksum checksum, int version) {
        super(id, fileRef, checksum);
        this.version = version;
    }

    public XmlRef(XmlRef xml, FileRef fileRef) {
        this(xml.getId(), fileRef, xml.getChecksum(), xml.getVersion());
    }
}