package cz.cas.lib.arcstorage.dto;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

public record RollbackInfo(AtomicBoolean shouldRollback, Lock lock) {
}
