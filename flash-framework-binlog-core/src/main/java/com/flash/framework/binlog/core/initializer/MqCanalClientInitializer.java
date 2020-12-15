package com.flash.framework.binlog.core.initializer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.flash.framework.binlog.core.autoconfigure.CanalConfigure;
import com.flash.framework.binlog.core.autoconfigure.ClientType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author zhurg
 * @date 2019/4/17 - 下午3:49
 */
@Slf4j
public class MqCanalClientInitializer extends CanalClientInitializer {


    public MqCanalClientInitializer(CanalConfigure canalConfigure) {
        super(canalConfigure);
    }

    @Override
    public void checkConfigure() throws IllegalArgumentException {
        if (ClientType.ROCKET_MQ.equals(canalConfigure.getClientType())) {
            Assert.notNull(canalConfigure.getRocketmq(), "[Binlog] properties binlog.rocketmq can not be null");
            Assert.notNull(canalConfigure.getRocketmq().getGroupName(), "[Binlog] properties binlog.rocketmq.group-name can not be null");
            Assert.notNull(canalConfigure.getRocketmq().getNameServer(), "[Binlog] properties binlog.rocketmq.name-server can not be null");
            Assert.notNull(canalConfigure.getRocketmq().getTopic(), "[Binlog] properties binlog.rocketmq.topic can not be null");
        } else if (ClientType.KAFKA.equals(canalConfigure.getClientType())) {
            Assert.notNull(canalConfigure.getKafka(), "[Binlog] properties binlog.kafka can not be null");
            Assert.notNull(canalConfigure.getKafka().getGroupId(), "[Binlog] properties binlog.kafka.group-id can not be null");
            Assert.notNull(canalConfigure.getKafka().getServers(), "[Binlog] properties binlog.kafka.servers can not be null");
            Assert.notNull(canalConfigure.getKafka().getTopic(), "[Binlog] properties binlog.kafka.topic can not be null");
        }
    }

    @Override
    public CanalConnector initClient() {
        CanalConnector connector = null;
        if (ClientType.ROCKET_MQ.equals(canalConfigure.getClientType())) {
            connector = new RocketMQCanalConnector(canalConfigure.getRocketmq().getNameServer(),
                    canalConfigure.getRocketmq().getTopic(), canalConfigure.getRocketmq().getGroupName(), canalConfigure.getBatchSize(), canalConfigure.getRocketmq().isFlatMessage());
        } else if (ClientType.KAFKA.equals(canalConfigure.getClientType())) {
            connector = new KafkaCanalConnector(canalConfigure.getKafka().getServers(), canalConfigure.getKafka().getTopic(),
                    canalConfigure.getKafka().getPartition(), canalConfigure.getKafka().getGroupId(), canalConfigure.getBatchSize(), canalConfigure.getKafka().isFlatMessage());
        }
        connector.connect();
        connector.subscribe();
        return connector;
    }

    @Override
    public void processor() throws Throwable {
        if (canalConnector instanceof RocketMQCanalConnector) {
            RocketMQCanalConnector rocketMQCanalConnector = (RocketMQCanalConnector) canalConnector;
            if (canalConfigure.getRocketmq().isFlatMessage()) {
                List<FlatMessage> messages = rocketMQCanalConnector.getFlatListWithoutAck(canalConfigure.getMqAckTimeout(), TimeUnit.MILLISECONDS);
                dealFlatMessage(messages);
            } else {
                List<Message> messages = rocketMQCanalConnector.getListWithoutAck(canalConfigure.getMqAckTimeout(), TimeUnit.MILLISECONDS);
                dealMessage(messages);
            }
            rocketMQCanalConnector.ack();
        } else if (canalConnector instanceof KafkaCanalConnector) {
            KafkaCanalConnector kafkaCanalConnector = (KafkaCanalConnector) canalConnector;
            if (canalConfigure.getKafka().isFlatMessage()) {
                List<FlatMessage> messages = kafkaCanalConnector.getFlatListWithoutAck(canalConfigure.getMqAckTimeout(), TimeUnit.MILLISECONDS);
                dealFlatMessage(messages);
            } else {
                List<Message> messages = kafkaCanalConnector.getListWithoutAck(canalConfigure.getMqAckTimeout(), TimeUnit.MILLISECONDS);
                dealMessage(messages);
            }
            kafkaCanalConnector.ack();
        }
    }

    private void dealMessage(List<Message> messages) throws Throwable {
        for (Message message : messages) {
            long batchId = message.getId();
            int size = message.getEntries().size();
            if (batchId != -1 && size > 0) {
                process(message.getEntries());
            }
        }
    }

    private void dealFlatMessage(List<FlatMessage> messages) throws Throwable {
        for (FlatMessage message : messages) {
            long batchId = message.getId();
            if (batchId != -1 && message.getData() != null) {
                process(message);
            }
        }
    }
}