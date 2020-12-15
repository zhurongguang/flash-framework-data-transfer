package com.flash.framework.binlog.plugin.es.elasticsearch.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * @author zhurg
 * @date 2020/12/7 - 下午8:53
 */
@Data
@EqualsAndHashCode
public abstract class IndexRequest implements Serializable {

    private static final long serialVersionUID = -6349017997504559513L;

    private String index;

    private String type;

    private String id;

    private Long version;

    private String versionType = "external_gte";
}