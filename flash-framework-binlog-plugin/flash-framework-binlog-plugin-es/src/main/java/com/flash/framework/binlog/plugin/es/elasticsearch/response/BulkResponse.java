package com.flash.framework.binlog.plugin.es.elasticsearch.response;

import com.flash.framework.binlog.plugin.es.elasticsearch.domain.BulkItemInfo;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午5:15
 */
@Data
public class BulkResponse implements Serializable {

    private static final long serialVersionUID = -6116707340122837612L;

    private Long took;

    private Boolean errors;

    private List<Map<String, BulkItemInfo>> items;
}