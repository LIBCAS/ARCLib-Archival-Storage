package cz.cas.lib.arcstorage.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class ThreadPoolProducer {

    private ExecutorService executorService;
    private ExecutorService batchOpsExecutorService;
    private ScheduledExecutorService scheduledExecutorService;

    public ThreadPoolProducer(@Value("${arcstorage.threadPools.batchOps}") int batchOpsThreadCount,
                              @Value("${arcstorage.threadPools.scheduled}") int scheduledThreadCount) {
        executorService = Executors.newCachedThreadPool();
        batchOpsExecutorService = Executors.newWorkStealingPool(batchOpsThreadCount);
        scheduledExecutorService = Executors.newScheduledThreadPool(scheduledThreadCount);
    }

    @Bean
    @Primary
    public ExecutorService executorService() {
        return executorService;
    }

    @Bean(name = "BatchOpsExecutorService")
    public ExecutorService reservedExecutorService() {
        return batchOpsExecutorService;
    }

    @Bean
    public ScheduledExecutorService scheduledExecutorService() {
        return scheduledExecutorService;
    }
}