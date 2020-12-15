package com.flash.framework.binlog.plugin.es.configure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午5:08
 */
@Data
@ConfigurationProperties(prefix = "binlog.es")
public class BinlogEsConfigure {

    /**
     * es address
     */
    private String address = "http://127.0.0.1:9200";

    /**
     * es 用户名
     */
    private String username;

    /**
     * es 密码
     */
    private String password;

    @NestedConfigurationProperty
    private HttpPoolConfigure httpPool;
}