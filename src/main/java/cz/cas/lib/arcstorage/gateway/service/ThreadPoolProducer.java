package cz.cas.lib.arcstorage.gateway.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ThreadPoolProducer {

    private ExecutorService executorService;

    public ThreadPoolProducer(@Value("${arcstorage.thread-count}") int threadCount) {
        executorService = Executors.newFixedThreadPool(threadCount);
    }

    @Bean
    public ExecutorService executorService() {
        return executorService;
    }
}