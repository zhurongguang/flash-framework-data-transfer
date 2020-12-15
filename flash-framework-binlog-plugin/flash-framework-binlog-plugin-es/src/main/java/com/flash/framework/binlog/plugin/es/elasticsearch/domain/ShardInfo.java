package com.flash.framework.binlog.plugin.es.elasticsearch.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * @author zhurg
 * @date 2020/12/8 - 上午11:23
 */
@Data
public class ShardInfo implements Serializable {

    private static final long serialVersionUID = 1765548077354686437L;

    private Long total;

    private Long successful;

    private Long failed;
}