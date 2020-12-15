package com.flash.framework.binlog.eventbus.listener;

import com.flash.framework.binlog.common.event.DataEvent;

/**
 * @author zhurg
 * @date 2020/11/18 - 下午5:49
 */
@FunctionalInterface
public interface BinlogEventListener {

    void onEvent(DataEvent event);
}