package cz.cas.lib.arcstorage.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import cz.cas.lib.arcstorage.gateway.dto.Checksum;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * SIP database entity. Its id is used in API calls and is projected into storage layer.
 */
@Entity
@Table(name = "arcstorage_aip_sip")
@NoArgsConstructor
public class AipSip extends ArchivalObject {

    @Override
    @JsonIgnore(false)
    @JsonProperty
    public String getId() {
        return super.getId();
    }

    @OneToMany(mappedBy = "sip", fetch = FetchType.EAGER)
    private List<AipXml> xmls = new ArrayList<>();

    @Setter
    @Getter
    @Enumerated(EnumType.STRING)
    private ObjectState state;

    public AipSip(String id) {
        this.id = id;
    }

    public AipSip(String id, Checksum checksum, ObjectState state) {
        super(id, checksum);
        this.state = state;
    }

    public AipSip(String id, Checksum checksum, ObjectState state, AipXml... xmls) {
        this(id, checksum, state);
        for (AipXml xml : xmls) {
            addXml(xml);
        }
    }

    private void addXml(AipXml aipXml) {
        xmls.add(aipXml);
    }

    public List<AipXml> getXmls() {
        return Collections.unmodifiableList(this.xmls);
    }

    public AipXml getXml(int i) {
        return this.xmls.get(i);
    }

    @JsonIgnore
    public AipXml getLatestXml() {
        return this.xmls.stream().max(Comparator.comparingInt(AipXml::getVersion)).get();
    }
}
