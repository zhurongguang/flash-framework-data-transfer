package com.flash.framework.datatransfer.autoconfigure;

import lombok.Data;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午5:12
 */
@Data
public class RocketMqClientConfigure {

    private String nameServer;

    private String topic;

    private String groupName;

    private boolean flatMessage;
}