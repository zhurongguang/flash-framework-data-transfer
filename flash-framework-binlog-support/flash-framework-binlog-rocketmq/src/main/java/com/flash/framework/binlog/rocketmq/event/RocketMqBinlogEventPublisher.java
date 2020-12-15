package com.flash.framework.binlog.rocketmq.event;

import com.alibaba.fastjson.JSON;
import com.flash.framework.binlog.core.event.BinlogEventPublisher;
import com.flash.framework.binlog.common.event.DataEvent;
import com.google.common.base.Throwables;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.stereotype.Component;

/**
 * @author zhurg
 * @date 2020/11/18 - 下午6:15
 */
@Slf4j
@Component
@AllArgsConstructor
public class RocketMqBinlogEventPublisher implements BinlogEventPublisher {

    private final RocketMQTemplate mqTemplate;

    /**
     * 按照topic为databse tag为table发送
     *
     * @param dataEvent
     */
    @Override
    public void publish(DataEvent dataEvent) {
        mqTemplate.asyncSend(String.format("%s:%s", dataEvent.getSchema(), dataEvent.getTable()), JSON.toJSONString(dataEvent), new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                if (log.isDebugEnabled()) {
                    log.debug("[Binlog] binlog event {} published", JSON.toJSONString(dataEvent));
                }
            }

            @Override
            public void onException(Throwable e) {
                if (log.isDebugEnabled()) {
                    log.debug("[Binlog] binlog event {} publish fialed,cause:{}", JSON.toJSONString(dataEvent), Throwables.getStackTraceAsString(e));
                }
            }
        });
    }
}
