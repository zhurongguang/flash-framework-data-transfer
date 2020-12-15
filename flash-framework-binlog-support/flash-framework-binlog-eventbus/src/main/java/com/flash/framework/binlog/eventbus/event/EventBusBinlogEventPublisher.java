package com.flash.framework.binlog.eventbus.event;

import com.alibaba.fastjson.JSON;
import com.flash.framework.binlog.core.event.BinlogEventPublisher;
import com.flash.framework.binlog.common.event.DataEvent;
import com.google.common.eventbus.EventBus;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author zhurg
 * @date 2020/11/18 - 下午5:46
 */
@Slf4j
@Component
@AllArgsConstructor
public class EventBusBinlogEventPublisher implements BinlogEventPublisher {

    private final EventBus eventBus;

    @Override
    public void publish(DataEvent dataEvent) {
        eventBus.post(dataEvent);
        if (log.isDebugEnabled()) {
            log.debug("[Binlog] binlog event {} published", JSON.toJSONString(dataEvent));
        }
    }
}
