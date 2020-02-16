package com.flash.framework.datatransfer.autoconfigure;

import lombok.Data;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午4:58
 */
@Data
public class SimpleClientConfigure {

    private String host;

    private Integer port = 11111;

    private String subscribe;
}