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
    private FileRef sip;

    private List<XmlFileRef> xmls = new ArrayList<>();

    public AipRef(AipRef aipRef, InputStream sipIs, InputStream xmlIs) {
        sip = new FileRef(aipRef.getSip().getId(), sipIs, aipRef.getSip().getChecksum());
        xmls.add(new XmlFileRef(aipRef.getXml().getId(), xmlIs, aipRef.getXml().getChecksum(), aipRef.getXml().getVersion()));
    }

    public List<XmlFileRef> getXmls() {
        return Collections.unmodifiableList(xmls);
    }

    public XmlFileRef getXml(int index) {
        return xmls.get(index);
    }

    public XmlFileRef getXml() {
        return xmls.get(0);
    }

    public void addXml(XmlFileRef xml) {
        xmls.add(xml);
    }
}
