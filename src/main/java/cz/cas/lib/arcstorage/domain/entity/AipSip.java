package cz.cas.lib.arcstorage.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import cz.cas.lib.arcstorage.dto.Checksum;
import cz.cas.lib.arcstorage.dto.ObjectState;
import lombok.NoArgsConstructor;

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
    private List<AipXml> xmls = new ArrayList<>();


    public AipSip(String id) {
        this.id = id;
    }

    public AipSip(String id, Checksum checksum, User owner, ObjectState state) {
        super(checksum, owner, state);
        this.id = id;
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
        return this.xmls.stream().max((a, b) -> {
            if (a.getCreated().isAfter(b.getCreated()))
                return 1;
            else
                return -1;
        }).get();
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
}
