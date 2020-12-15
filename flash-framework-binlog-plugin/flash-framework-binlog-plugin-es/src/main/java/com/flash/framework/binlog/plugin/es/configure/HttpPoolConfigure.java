package com.flash.framework.binlog.plugin.es.configure;

import lombok.Data;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午5:10
 */
@Data
public class HttpPoolConfigure {

    private Long callTimeout = 10 * 1000L;

    private Long readTimeout = 10 * 1000L;

    private Long writeTimeout = 10 * 1000L;

    private Long connectTimeout = 10 * 1000L;

    private Integer maxIdleConnections = 10;

    private Long keepAlive = 300000L;
}