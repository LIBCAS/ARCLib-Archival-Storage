package cz.cas.lib.arcstorage.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.BatchSize;

import javax.persistence.*;

@Getter
@Setter
@BatchSize(size = 100)
@Entity
@Table(name = "arclib_aip_xml")
/**
 * XML database entity. Its id is used only for internal purpose and is neither accessible over API nor projected to storage.
 */
public class AipXml extends ArchivalObject {

    @ManyToOne
    @JoinColumn(name = "arclib_aip_sip_id")
    @JsonIgnore
    private AipSip sip;
    private int version;

    @Enumerated(EnumType.STRING)
    private XmlState state;

    public AipXml() {
        super(null, null);
    }

    public AipXml(String id, String md5, AipSip sip, int version, XmlState state) {
        super(id, md5);
        this.sip = sip;
        this.version = version;
        this.state = state;
    }

    public AipXml(String md5, AipSip sip, int version, XmlState state) {
        super(md5);
        this.sip = sip;
        this.version = version;
        this.state = state;
    }
}
