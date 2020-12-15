package com.flash.framework.binlog.plugin.es.elasticsearch.request;

import com.google.common.collect.Sets;
import lombok.Data;

import java.io.Serializable;
import java.util.Set;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午5:15
 */
@Data
public class BulkRequest implements Serializable {

    private static final long serialVersionUID = -3626362138992017218L;

    private Set<IndexRequest> requests;

    private String refresh;

    public BulkRequest() {
        this.requests = Sets.newHashSet();
    }

    public BulkRequest add(IndexRequest request) {
        this.requests.add(request);
        return this;
    }
}