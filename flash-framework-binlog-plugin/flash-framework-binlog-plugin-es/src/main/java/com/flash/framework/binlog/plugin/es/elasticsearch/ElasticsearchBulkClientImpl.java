package com.flash.framework.binlog.plugin.es.elasticsearch;

import com.flash.framework.binlog.plugin.es.configure.BinlogEsConfigure;
import com.flash.framework.binlog.plugin.es.elasticsearch.request.BulkRequest;
import com.flash.framework.binlog.plugin.es.elasticsearch.request.DeleteRequest;
import com.flash.framework.binlog.plugin.es.elasticsearch.request.UpsertRequest;
import com.flash.framework.binlog.plugin.es.elasticsearch.response.BulkResponse;
import com.flash.framework.binlog.plugin.es.http.HttpClient;
import com.flash.framework.binlog.plugin.es.utils.JsonUtil;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import static com.flash.framework.binlog.plugin.es.EsPluginConstant.AUTHORIZATION;
import static com.flash.framework.binlog.plugin.es.EsPluginConstant.LINE_SEPARATOR;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午7:59
 */
@Component
@AllArgsConstructor
public class ElasticsearchBulkClientImpl implements ElasticsearchBulkClient {

    private final HttpClient httpClient;

    private final BinlogEsConfigure binlogEsConfigure;

    @Override
    public BulkResponse bulk(BulkRequest bulkRequest) throws Exception {
        StringBuffer url = new StringBuffer(binlogEsConfigure.getAddress());
        url.append("/_bulk");
        if (StringUtils.isNotBlank(bulkRequest.getRefresh())) {
            url.append("?").append(bulkRequest.getRefresh());
        }

        Map<String, String> headers = Maps.newHashMap();

        String authorization = getAuthorization();
        if (StringUtils.isNotBlank(authorization)) {
            headers.put(AUTHORIZATION, authorization);
        }


        String response = httpClient.postJson(url.toString(), headers, body(bulkRequest));
        BulkResponse bulkResponse = JsonUtil.fromJson(response, BulkResponse.class);
        return bulkResponse;
    }

    public String getAuthorization() {
        if (StringUtils.isNotBlank(binlogEsConfigure.getUsername()) &&
                StringUtils.isNotBlank(binlogEsConfigure.getPassword())) {
            return toBase64();
        }

        return null;
    }

    private String body(BulkRequest request) {
        StringBuilder body = new StringBuilder();
        request.getRequests().forEach(req -> {
            if (req instanceof UpsertRequest) {
                body.append(upsert((UpsertRequest) req));
            } else if (req instanceof DeleteRequest) {
                body.append(delete((DeleteRequest) req));
            }
        });
        return body.toString();
    }

    private String upsert(UpsertRequest req) {
        Map<String, Object> index = Maps.newHashMapWithExpectedSize(5);
        if (StringUtils.isNotBlank(req.getIndex())) {
            index.put("_index", req.getIndex());
        }

        if (StringUtils.isNotBlank(req.getType())) {
            index.put("_type", req.getType());
        }

        if (StringUtils.isNotBlank(req.getId())) {
            index.put("_id", req.getId());
        }

        if (null != req.getVersion()) {
            index.put("_version_type", req.getVersionType());
            index.put("_version", req.getVersion());
        }

        StringBuilder sb = new StringBuilder();
        sb.append(JsonUtil.toJson(Collections.singletonMap("index", index))).append(LINE_SEPARATOR);
        sb.append(JsonUtil.toJson(req.getSource())).append(LINE_SEPARATOR);
        return sb.toString();
    }

    private String delete(DeleteRequest req) {
        Map<String, Object> delete = Maps.newHashMapWithExpectedSize(5);
        delete.put("_index", req.getIndex());
        if (!org.springframework.util.StringUtils.isEmpty(req.getType())) {
            delete.put("_type", req.getType());
        }

        if (!org.springframework.util.StringUtils.isEmpty(req.getId())) {
            delete.put("_id", req.getId());
        }

        if (null != req.getVersion()) {
            delete.put("_version_type", req.getVersionType());
            delete.put("_version", req.getVersion());
        }

        return JsonUtil.toJson(Collections.singletonMap("delete", delete)) + LINE_SEPARATOR;
    }

    private String toBase64() {
        byte[] bytes = (binlogEsConfigure.getUsername() + ":" + binlogEsConfigure.getPassword()).getBytes();
        return "Basic " + new String(Base64.getEncoder().encode(bytes), Charset.forName("UTF-8"));
    }
}
