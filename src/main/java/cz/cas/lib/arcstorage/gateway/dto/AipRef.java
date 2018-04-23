package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@NoArgsConstructor
public class AipRef {
    @Setter
    @Getter
    private ArchiveFileRef sip;

    private List<XmlRef> xmls = new ArrayList<>();

    public AipRef(String sipId, InputStream sipStream, Checksum sipChecksum, InputStream aipXmlStream, Checksum xmlChecksum) {
        sip = new ArchiveFileRef(sipId, new FileRef(sipStream), sipChecksum);
        xmls.add(new XmlRef(new FileRef(aipXmlStream), xmlChecksum, 1));
    }

    public AipRef(AipRef aipRef, FileRef sipIs, FileRef xmlIs) {
        sip = new ArchiveFileRef(aipRef.getSip().getId(), sipIs, aipRef.getSip().getChecksum());
        xmls.add(new XmlRef(aipRef.getXml().getId(), xmlIs, aipRef.getXml().getChecksum(), aipRef.getXml().getVersion()));
    }

    public List<XmlRef> getXmls() {
        return Collections.unmodifiableList(xmls);
    }

    public XmlRef getXml(int index) {
        return xmls.get(index);
    }

    /**
     * Used when the aip has only one XML i.e. it is just under process related to creation.
     *
     * @return
     */
    public XmlRef getXml() {
        return xmls.get(0);
    }

    public void addXml(XmlRef xml) {
        xmls.add(xml);
    }
}
