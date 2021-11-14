package cz.cas.lib.arcstorage.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import cz.cas.lib.arcstorage.dto.ArchivalObjectDto;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

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
    @Setter
    private List<AipXml> xmls = new ArrayList<>();


    public AipSip(String id) {
        this.id = id;
    }

    public AipSip(String id, Checksum checksum, User owner, ObjectState state) {
        super(checksum, owner, state);
        this.id = id;
    }

    public void addXml(AipXml aipXml) {
        xmls.add(aipXml);
    }

    public List<AipXml> getXmls() {
        return Collections.unmodifiableList(this.xmls);
    }

    public List<AipXml> getXmlsSortedByVersionAsc() {
        ArrayList<AipXml> xmlsCopy = new ArrayList<>(this.xmls);
        xmlsCopy.sort(Comparator.comparing(AipXml::getCreated));
        return Collections.unmodifiableList(xmlsCopy);
    }

    public AipXml getXml(int i) {
        return this.xmls.get(i);
    }

    @JsonIgnore
    public AipXml getLatestXml() {
        switch (this.xmls.size()) {
            case 0:
                throw new IllegalStateException();
            case 1:
                return this.xmls.get(0);
            default:
                AipXml latestAccordingToTimestamp = this.xmls.stream().max((a, b) -> {
                    if (a.getCreated().isAfter(b.getCreated()))
                        return 1;
                    else
                        return -1;
                }).get();
                AipXml latestAccordingToVersionNumber = this.xmls.stream().max(Comparator.comparingInt(AipXml::getVersion)).get();
                if (!latestAccordingToTimestamp.equals(latestAccordingToVersionNumber) && latestAccordingToTimestamp.getVersion() != latestAccordingToVersionNumber.getVersion())
                    throw new IllegalStateException("xml " + latestAccordingToTimestamp.getId() + " with lower version number (" + latestAccordingToTimestamp.getVersion() + ") is more recent than xml " + latestAccordingToVersionNumber.getId() + " with version number " + latestAccordingToVersionNumber.getVersion());
                return latestAccordingToTimestamp;
        }
    }

    @JsonIgnore
    public List<AipXml> getArchivedXmls() {
        return this.xmls.stream()
                .filter(xml -> xml.getState() == ObjectState.ARCHIVED)
                .collect(Collectors.toList());
    }

    @JsonIgnore
    public AipXml getLatestArchivedXml() {
        return this.xmls.stream()
                .filter(xml -> xml.getState() == ObjectState.ARCHIVED)
                .max(Comparator.comparingInt(AipXml::getVersion)).get();
    }

    @Override
    public ArchivalObjectDto toDto() {
        return new ArchivalObjectDto(id, id, getChecksum(), getOwner(), null, getState(), getCreated(), ObjectType.SIP);
    }
}
