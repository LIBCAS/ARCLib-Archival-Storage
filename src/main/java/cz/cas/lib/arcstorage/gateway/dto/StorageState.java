package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class StorageState extends BasicStateInfo {

    private StorageType type;

    private List<NodeState> nodes = new ArrayList<>();

    public void addNode(NodeState node) {
        this.nodes.add(node);
    }

    public StorageState(long capacity, long free, boolean running, StorageType type) {
        super(capacity, free, running);
        this.setType(type);
    }
}
