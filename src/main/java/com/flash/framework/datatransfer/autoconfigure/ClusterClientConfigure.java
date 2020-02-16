package com.flash.framework.datatransfer.autoconfigure;

import lombok.Data;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午5:05
 */
@Data
public class ClusterClientConfigure {

    private String zkHosts;

    private boolean enableMultiClient;

    private String subscribe;
}