package com.flash.framework.binlog.plugin.es.elasticsearch.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午8:59
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class DeleteRequest extends IndexRequest {

    private static final long serialVersionUID = 1935716161642873974L;
}