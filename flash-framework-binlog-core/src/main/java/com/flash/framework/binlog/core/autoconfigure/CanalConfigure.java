package com.flash.framework.binlog.core.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午4:49
 */
@Data
@ConfigurationProperties(prefix = "binlog")
public class CanalConfigure {

    /**
     * Canal client类型
     */
    private ClientType clientType = ClientType.DEFAULT;

    /**
     * canal dest
     */
    private String destination = "example";

    /**
     * canal database username
     */
    private String username;

    /**
     * canal database password
     */
    private String password;

    /**
     * canal address
     */
    private String address;

    /**
     * canal filterRegex
     */
    private String filterRegex;

    /**
     * 需要过滤的表
     */
    private String filterTables;

    private long healthCheckTime = 1000 * 5L;

    private int batchSize = 1024;

    private long mqAckTimeout = 100L;

    private long idelSleepTime = 1000L;

    private long blockWaitMillis = 500L;

    @NestedConfigurationProperty
    private KafkaClientConfigure kafka;

    @NestedConfigurationProperty
    private RocketMqClientConfigure rocketmq;
}