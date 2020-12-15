package com.flash.framework.binlog.common.event;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author zhurg
 * @date 2020/11/17 - 上午11:16
 */
@Data
public class DataEvent implements Serializable {

    private static final long serialVersionUID = 8549704996337913734L;

    /**
     * database
     */
    private String schema;

    /**
     * table
     */
    private String table;

    /**
     * sql event type
     */
    private EventType eventType;

    /**
     * rows
     */
    private List<RowEvent> rows;
}