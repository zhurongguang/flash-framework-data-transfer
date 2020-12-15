package com.flash.framework.binlog.kafka.event;

import com.alibaba.fastjson.JSON;
import com.flash.framework.binlog.common.event.DataEvent;
import com.flash.framework.binlog.core.event.BinlogEventPublisher;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * @author zhurg
 * @date 2020/12/4 - 下午4:12
 */
@Slf4j
@Component
@AllArgsConstructor
public class KafkaBinlogEventPublisher implements BinlogEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void publish(DataEvent dataEvent) {
        //按照数据库维度发送消息
        kafkaTemplate.send(dataEvent.getSchema(), JSON.toJSONString(dataEvent));
        if (log.isDebugEnabled()) {
            log.debug("[Binlog] binlog event {} published", JSON.toJSONString(dataEvent));
        }
    }
}
