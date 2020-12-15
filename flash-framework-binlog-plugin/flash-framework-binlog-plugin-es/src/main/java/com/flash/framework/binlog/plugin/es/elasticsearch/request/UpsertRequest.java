package com.flash.framework.binlog.plugin.es.elasticsearch.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午9:00
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class UpsertRequest extends IndexRequest {

    private static final long serialVersionUID = -5393048410326559889L;

    private Object source;
}