package com.flash.framework.binlog.plugin.core.handler;

import com.flash.framework.binlog.common.event.DataEvent;

/**
 * @author zhurg
 * @date 2020/12/8 - 上午11:42
 */
@FunctionalInterface
public interface BinlogEventHandler {

    void onEvent(DataEvent dataEvent);
}