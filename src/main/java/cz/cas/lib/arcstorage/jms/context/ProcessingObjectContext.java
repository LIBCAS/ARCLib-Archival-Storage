package cz.cas.lib.arcstorage.jms.context;

import cz.cas.lib.arcstorage.jms.JmsAction;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicBoolean;

@ToString
@AllArgsConstructor
@Getter
public class ProcessingObjectContext {
    private String messageId;
    private String objectDbId;
    private JmsAction action;
    private AtomicBoolean stopSignal;
    private AtomicBoolean parallelRequestFailed;
}
