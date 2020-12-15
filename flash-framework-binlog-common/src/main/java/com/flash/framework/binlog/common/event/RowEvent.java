package com.flash.framework.binlog.common.event;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * @author zhurg
 */
@Data
public class RowEvent implements Serializable {

    private static final long serialVersionUID = 4008250122100752051L;

    private Map<String, Object> before;

    private Map<String, Object> after;
}