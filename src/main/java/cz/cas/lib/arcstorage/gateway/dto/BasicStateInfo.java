package cz.cas.lib.arcstorage.gateway.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class BasicStateInfo {
    private long capacity;
    private long used;
    private long free;
    private boolean running;

    public BasicStateInfo(long capacity, long free, boolean running) {
        this.capacity = capacity;
        this.free = free;
        this.used = capacity - free;
        this.running = running;
    }
}

