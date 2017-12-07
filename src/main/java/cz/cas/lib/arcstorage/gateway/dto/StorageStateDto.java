package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class StorageStateDto extends BasicStateInfo {

    private StorageType type;

    private List<NodeStateDto> nodes = new ArrayList<>();

    public void addNode(NodeStateDto node) {
        this.nodes.add(node);
    }

    public StorageStateDto(long capacity, long free, boolean running, StorageType type) {
        super(capacity, free, running);
        this.setType(type);
    }
}
