package com.flash.framework.binlog.plugin.es;

import com.flash.framework.binlog.plugin.es.configure.BinlogEsConfigure;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午5:06
 */
@Configuration
@ComponentScan
@EnableConfigurationProperties({BinlogEsConfigure.class})
public class BinlogEsPluginConfiguration {
}