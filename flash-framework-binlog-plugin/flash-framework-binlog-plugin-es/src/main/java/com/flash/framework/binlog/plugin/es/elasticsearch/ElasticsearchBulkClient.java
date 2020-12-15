package com.flash.framework.binlog.plugin.es.elasticsearch;

import com.flash.framework.binlog.plugin.es.elasticsearch.request.BulkRequest;
import com.flash.framework.binlog.plugin.es.elasticsearch.response.BulkResponse;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午5:13
 */
@FunctionalInterface
public interface ElasticsearchBulkClient {

    /**
     * Elasticsearch bulk api
     *
     * @param bulkRequest
     * @return
     * @throws Exception
     */
    BulkResponse bulk(BulkRequest bulkRequest) throws Exception;
}