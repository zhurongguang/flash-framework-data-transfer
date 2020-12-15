package com.flash.framework.binlog.plugin.es.http;

import java.util.Map;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午5:16
 */
public interface HttpClient {

    String post(String url, Map<String, String> headers, Map<String, String> params) throws Exception;

    String postJson(String url, Map<String, String> headers, String json) throws Exception;

    String get(String url, Map<String, String> headers, Map<String, String> params) throws Exception;
}