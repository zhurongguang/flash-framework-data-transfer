package com.flash.framework.datatransfer.initializer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.flash.framework.datatransfer.autoconfigure.ClientType;
import com.flash.framework.datatransfer.autoconfigure.DataTransferConfigure;
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


    public MqCanalClientInitializer(DataTransferConfigure dataTransferConfigure) {
        super(dataTransferConfigure);
    }

    @Override
    public void checkConfigure() throws IllegalArgumentException {
        if (ClientType.RocketMQ.equals(dataTransferConfigure.getClientType())) {
            Assert.notNull(dataTransferConfigure.getRocketmq(), "[DataTransfer] Simple client property datatransfer.rocketmq can not be null");
            Assert.notNull(dataTransferConfigure.getRocketmq().getGroupName(), "[DataTransfer] Simple client property datatransfer.rocketmq.group-name can not be null");
            Assert.notNull(dataTransferConfigure.getRocketmq().getNameServer(), "[DataTransfer] Simple client property datatransfer.rocketmq.name-server can not be null");
            Assert.notNull(dataTransferConfigure.getRocketmq().getTopic(), "[DataTransfer] Simple client property datatransfer.rocketmq.topic can not be null");
        } else if (ClientType.Kafka.equals(dataTransferConfigure.getClientType())) {
            Assert.notNull(dataTransferConfigure.getKafka(), "[DataTransfer] Simple client property datatransfer.kafka can not be null");
            Assert.notNull(dataTransferConfigure.getKafka().getGroupId(), "[DataTransfer] Simple client property datatransfer.kafka.group-id can not be null");
            Assert.notNull(dataTransferConfigure.getKafka().getServers(), "[DataTransfer] Simple client property datatransfer.kafka.servers can not be null");
            Assert.notNull(dataTransferConfigure.getKafka().getTopic(), "[DataTransfer] Simple client property datatransfer.kafka.topic can not be null");
        }
    }

    @Override
    public CanalConnector initClient() {
        CanalConnector connector = null;
        if (ClientType.RocketMQ.equals(dataTransferConfigure.getClientType())) {
            connector = new RocketMQCanalConnector(dataTransferConfigure.getRocketmq().getNameServer(),
                    dataTransferConfigure.getRocketmq().getTopic(), dataTransferConfigure.getRocketmq().getGroupName(), dataTransferConfigure.getBatchSize(), dataTransferConfigure.getRocketmq().isFlatMessage());
        } else if (ClientType.Kafka.equals(dataTransferConfigure.getClientType())) {
            connector = new KafkaCanalConnector(dataTransferConfigure.getKafka().getServers(), dataTransferConfigure.getKafka().getTopic(),
                    dataTransferConfigure.getKafka().getPartition(), dataTransferConfigure.getKafka().getGroupId(), dataTransferConfigure.getBatchSize(), dataTransferConfigure.getKafka().isFlatMessage());
        }
        connector.connect();
        connector.subscribe();
        return connector;
    }

    @Override
    public void processor() throws Throwable {
        if (canalConnector instanceof RocketMQCanalConnector) {
            RocketMQCanalConnector rocketMQCanalConnector = (RocketMQCanalConnector) canalConnector;
            if (dataTransferConfigure.getRocketmq().isFlatMessage()) {
                List<FlatMessage> messages = rocketMQCanalConnector.getFlatListWithoutAck(dataTransferConfigure.getMqAckTimeout(), TimeUnit.MILLISECONDS);
                dealFlatMessage(messages);
            } else {
                List<Message> messages = rocketMQCanalConnector.getListWithoutAck(dataTransferConfigure.getMqAckTimeout(), TimeUnit.MILLISECONDS);
                dealMessage(messages);
            }
            rocketMQCanalConnector.ack();
        } else if (canalConnector instanceof KafkaCanalConnector) {
            KafkaCanalConnector kafkaCanalConnector = (KafkaCanalConnector) canalConnector;
            if (dataTransferConfigure.getKafka().isFlatMessage()) {
                List<FlatMessage> messages = kafkaCanalConnector.getFlatListWithoutAck(dataTransferConfigure.getMqAckTimeout(), TimeUnit.MILLISECONDS);
                dealFlatMessage(messages);
            } else {
                List<Message> messages = kafkaCanalConnector.getListWithoutAck(dataTransferConfigure.getMqAckTimeout(), TimeUnit.MILLISECONDS);
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
                processor(message.getEntries());
            }
        }
    }

    private void dealFlatMessage(List<FlatMessage> messages) throws Throwable {
        for (FlatMessage message : messages) {
            long batchId = message.getId();
            if (batchId != -1 && message.getData() != null) {
                processor(message);
            }
        }
    }
}