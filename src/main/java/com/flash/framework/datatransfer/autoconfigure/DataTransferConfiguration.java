package com.flash.framework.datatransfer.autoconfigure;

import com.flash.framework.datatransfer.initializer.MqCanalClientInitializer;
import com.flash.framework.datatransfer.initializer.SimpleCanalClientInitializer;
import com.flash.framework.datatransfer.processor.ProcessorManager;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author zhurg
 * @date 2019/4/17 - 下午3:38
 */
@Configuration
public class DataTransferConfiguration {

    @Bean
    public DataTransferConfigure dataTransferConfigure() {
        return new DataTransferConfigure();
    }

    @Bean
    @ConditionalOnMissingBean
    public ProcessorManager processorManager() {
        return new ProcessorManager();
    }

    @Bean(name = "canalClientInitializer")
    @ConditionalOnExpression("'${datatransfer.client-type}'.equals('Simple') || '${datatransfer.client-type}'.equals('Cluster')")
    public SimpleCanalClientInitializer simpleCanalClientInitializer(DataTransferConfigure dataTransferConfigure) {
        return new SimpleCanalClientInitializer(dataTransferConfigure);
    }

    @Bean(name = "canalClientInitializer")
    @ConditionalOnExpression("'${datatransfer.client-type}'.equals('RocketMQ') || '${datatransfer.client-type}'.equals('Kafka')")
    public MqCanalClientInitializer mqCanalClientInitializer(DataTransferConfigure dataTransferConfigure) {
        return new MqCanalClientInitializer(dataTransferConfigure);
    }
}