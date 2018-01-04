package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NodeState extends BasicStateInfo {
    String id;
    String ip;

    public NodeState(long capacity, long free, boolean running, String id, String ip) {
        super(capacity, free, running);
        this.setId(id);
        this.setIp(ip);
    }
}