package cz.cas.lib.arcstorage.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ThreadPoolProducer {

    private ExecutorService executorService;
    private ExecutorService reservedExecutorService;

    public ThreadPoolProducer(@Value("${arcstorage.thread-count}") int threadCount,
                              @Value("${arcstorage.reserved-thread-count}") int reservedThreadCount) {
        executorService = Executors.newFixedThreadPool(threadCount);
        reservedExecutorService = Executors.newFixedThreadPool(reservedThreadCount);
    }

    @Bean
    @Primary
    public ExecutorService executorService() {
        return executorService;
    }

    @Bean(name = "ReservedExecutorService")
    public ExecutorService reservedExecutorService() {
        return reservedExecutorService;
    }
}