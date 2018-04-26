package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

/**
 * DTO for transfer of XML object with its file content. Contains XML version in addition to other attributes of {@link ArchivalObjectDto}.
 * Id of the XML on the service layer has no use on storage layer where the XML is identified by SIP ID + XML version number.
 */
@Getter
@Setter
public class XmlDto extends ArchivalObjectDto {
    private int version;

    public XmlDto(FileContentDto fileContentDto, Checksum checksum, int version) {
        super(null, fileContentDto, checksum);
        this.version = version;
    }

    public XmlDto(String id, FileContentDto fileContentDto, Checksum checksum, int version) {
        super(id, fileContentDto, checksum);
        this.version = version;
    }

    public XmlDto(XmlDto xml, FileContentDto fileContentDto) {
        this(xml.getId(), fileContentDto, xml.getChecksum(), xml.getVersion());
    }
}
