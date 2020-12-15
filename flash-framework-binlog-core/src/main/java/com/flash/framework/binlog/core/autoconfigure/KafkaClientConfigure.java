package com.flash.framework.binlog.core.autoconfigure;

import lombok.Data;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午5:08
 */
@Data
public class KafkaClientConfigure {

    private String servers;

    private String topic;

    private Integer partition;

    private String groupId;

    private boolean flatMessage;

}