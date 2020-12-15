package com.flash.framework.binlog.core.event;

import com.flash.framework.binlog.common.event.DataEvent;

/**
 * @author zhurg
 * @date 2020/11/18 - 下午5:14
 */
@FunctionalInterface
public interface BinlogEventPublisher {

    /**
     * publish binlog event
     *
     * @param dataEvent
     */
    void publish(DataEvent dataEvent);
}