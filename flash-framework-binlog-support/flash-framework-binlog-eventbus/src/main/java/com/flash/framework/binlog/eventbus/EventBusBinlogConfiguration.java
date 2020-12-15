package com.flash.framework.binlog.eventbus;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhurg
 * @date 2020/11/18 - 下午5:40
 */
@Slf4j
@Configuration
@ComponentScan
public class EventBusBinlogConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public EventBus eventBus() {
        return new AsyncEventBus(
                new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 5,
                        Runtime.getRuntime().availableProcessors() * 10,
                        60L,
                        TimeUnit.SECONDS,
                        new ArrayBlockingQueue<>(10240),
                        (new ThreadFactoryBuilder()).setNameFormat("binlog-event-%d").build(),
                        (r, executor) -> log.error("binlog event {} is rejected", r)));
    }
}