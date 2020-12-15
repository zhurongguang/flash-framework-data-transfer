package com.flash.framework.binlog.plugin.es.http;

import com.flash.framework.binlog.plugin.es.configure.BinlogEsConfigure;
import com.flash.framework.binlog.plugin.es.configure.HttpPoolConfigure;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import okhttp3.*;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午5:16
 */
@Component
public class OkHttpClientImpl implements HttpClient {

    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    private final BinlogEsConfigure binlogEsConfigure;

    private OkHttpClient okHttpClient;

    public OkHttpClientImpl(BinlogEsConfigure binlogEsConfigure) {
        this.binlogEsConfigure = binlogEsConfigure;
    }

    @PostConstruct
    public void init() {
        HttpPoolConfigure httpPoolConfigure = binlogEsConfigure.getHttpPool();
        if (Objects.isNull(httpPoolConfigure)) {
            httpPoolConfigure = new HttpPoolConfigure();
        }
        this.okHttpClient = new OkHttpClient.Builder()
                .callTimeout(httpPoolConfigure.getCallTimeout(), TimeUnit.MILLISECONDS)
                .readTimeout(httpPoolConfigure.getReadTimeout(), TimeUnit.MILLISECONDS)
                .writeTimeout(httpPoolConfigure.getWriteTimeout(), TimeUnit.MILLISECONDS)
                .connectTimeout(httpPoolConfigure.getConnectTimeout(), TimeUnit.MILLISECONDS)
                .connectionPool(new ConnectionPool(httpPoolConfigure.getMaxIdleConnections(), httpPoolConfigure.getKeepAlive(), TimeUnit.MILLISECONDS))
                .build();
    }

    @Override
    public String post(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
        Request.Builder builder = new Request.Builder()
                .url(url);

        if (MapUtils.isNotEmpty(headers)) {
            builder.headers(Headers.of(headers));
        }

        if (MapUtils.isNotEmpty(params)) {
            FormBody.Builder formBuilder = new FormBody.Builder();
            params.forEach((k, v) -> formBuilder.add(k, v));
        }

        Response response = okHttpClient.newCall(builder.build()).execute();
        String body = null == response.body() ? null : response.body().string();
        if (response.isSuccessful() && StringUtils.isNotBlank(body)) {
            return body;
        } else {
            throw new RuntimeException(body);
        }
    }

    @Override
    public String postJson(String url, Map<String, String> headers, String json) throws Exception {
        Request.Builder builder = new Request.Builder()
                .url(url);

        if (MapUtils.isNotEmpty(headers)) {
            builder.headers(Headers.of(headers));
        }

        builder.post(RequestBody.create(json, JSON));

        Response response = okHttpClient.newCall(builder.build()).execute();
        String body = null == response.body() ? null : response.body().string();
        if (response.isSuccessful() && StringUtils.isNotBlank(body)) {
            return body;
        } else {
            throw new RuntimeException(body);
        }
    }

    @Override
    public String get(String url, Map<String, String> headers, Map<String, String> params) throws Exception {
        StringBuffer sb = new StringBuffer(url);

        if (MapUtils.isNotEmpty(params)) {
            sb.append("?");
            List<String> args = Lists.newArrayListWithCapacity(params.size());
            params.forEach((k, v) -> {
                if (StringUtils.isNotBlank(v)) {
                    args.add(k + "=" + v);
                }
            });
            sb.append(Joiner.on("&").join(args));
        }

        Request.Builder builder = new Request.Builder()
                .url(sb.toString());

        if (MapUtils.isNotEmpty(headers)) {
            builder.headers(Headers.of(headers));
        }

        builder.get();

        Response response = okHttpClient.newCall(builder.build()).execute();
        String body = null == response.body() ? null : response.body().string();
        if (response.isSuccessful() && StringUtils.isNotBlank(body)) {
            return body;
        } else {
            throw new RuntimeException(body);
        }
    }
}