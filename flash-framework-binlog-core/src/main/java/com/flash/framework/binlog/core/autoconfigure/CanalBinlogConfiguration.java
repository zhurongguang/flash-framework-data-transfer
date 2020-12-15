package com.flash.framework.binlog.core.autoconfigure;

import com.flash.framework.binlog.core.initializer.CanalClientInitializer;
import com.flash.framework.binlog.core.initializer.MqCanalClientInitializer;
import com.flash.framework.binlog.core.initializer.SimpleCanalClientInitializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author zhurg
 * @date 2019/4/17 - 下午3:38
 */
@Configuration
@EnableConfigurationProperties(CanalConfigure.class)
public class CanalBinlogConfiguration {

    @Bean(name = "canalClientInitializer")
    public CanalClientInitializer canalClientInitializer(CanalConfigure canalConfigure) {
        if (ClientType.DEFAULT.equals(canalConfigure.getClientType())) {
            return new SimpleCanalClientInitializer(canalConfigure);
        } else {
            return new MqCanalClientInitializer(canalConfigure);
        }
    }
}