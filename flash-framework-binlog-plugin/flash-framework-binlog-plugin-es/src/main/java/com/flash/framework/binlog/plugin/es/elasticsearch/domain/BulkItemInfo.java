package com.flash.framework.binlog.plugin.es.elasticsearch.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.io.Serializable;

/**
 * @author zhurg
 * @date 2020/12/8 - 上午11:19
 */
@Data
public class BulkItemInfo implements Serializable {

    private static final long serialVersionUID = 4601501584245674543L;

    @JsonProperty("_index")
    private String index;

    @JsonProperty("_type")
    private String type;

    @JsonProperty("_id")
    private String id;

    @JsonProperty("_version")
    private Long version;

    private String result;

    @JsonProperty("_shards")
    private ShardInfo shards;

    private Integer status;

    @JsonProperty("_seq_no")
    private Integer seqNo;

    @JsonProperty("_primary_term")
    private Integer primaryTerm;
}