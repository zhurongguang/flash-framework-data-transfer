package com.flash.framework.datatransfer.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午4:49
 */
@Data
@ConfigurationProperties(prefix = "datatransfer")
public class DataTransferConfigure {

    /**
     * Canal client类型
     */
    private ClientType clientType = ClientType.Simple;

    private String destination = "example";

    private String username;

    private String password;

    private int retryTimes = 5;

    private long retryIntervalTime = 1000 * 20L;

    private int batchSize = 1024;

    private long mqAckTimeout = 100L;

    private long emptySleepTime = 1000L;

    @NestedConfigurationProperty
    private SimpleClientConfigure simple;

    @NestedConfigurationProperty
    private ClusterClientConfigure cluster;

    @NestedConfigurationProperty
    private KafkaClientConfigure kafka;

    @NestedConfigurationProperty
    private RocketMqClientConfigure rocketmq;
}