package com.flash.framework.datatransfer.initializer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.flash.framework.datatransfer.autoconfigure.ClientType;
import com.flash.framework.datatransfer.autoconfigure.DataTransferConfigure;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.net.InetSocketAddress;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午5:17
 */
@Slf4j
public class SimpleCanalClientInitializer extends CanalClientInitializer {

    public SimpleCanalClientInitializer(DataTransferConfigure dataTransferConfigure) {
        super(dataTransferConfigure);
    }

    @Override
    public void checkConfigure() throws IllegalArgumentException {
        if (ClientType.Simple.equals(dataTransferConfigure.getClientType())) {
            Assert.notNull(dataTransferConfigure.getSimple(), "[DataTransfer] Simple client property datatransfer.simple can not be null");
            Assert.notNull(dataTransferConfigure.getSimple().getHost(), "[DataTransfer] Simple client property datatransfer.simple.host can not be null");
            Assert.notNull(dataTransferConfigure.getSimple().getSubscribe(), "[DataTransfer] Simple client property datatransfer.simple.subscribe can not be null");
        } else if (ClientType.Cluster.equals(dataTransferConfigure.getClientType())) {
            Assert.notNull(dataTransferConfigure.getCluster(), "[DataTransfer] Simple client property datatransfer.cluster can not be null");
            Assert.notNull(dataTransferConfigure.getCluster().getZkHosts(), "[DataTransfer] Simple client property datatransfer.cluster.zk-hosts can not be null");
            Assert.notNull(dataTransferConfigure.getSimple().getSubscribe(), "[DataTransfer] Simple client property datatransfer.cluster.subscribe can not be null");
        }
        if (dataTransferConfigure.getBatchSize() <= 0) {
            dataTransferConfigure.setBatchSize(1024);
        }
    }

    @Override
    public CanalConnector initClient() {
        CanalConnector connector = null;
        if (ClientType.Simple.equals(dataTransferConfigure.getClientType())) {
            connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(dataTransferConfigure.getSimple().getHost(), dataTransferConfigure.getSimple().getPort()), dataTransferConfigure.getDestination(),
                    dataTransferConfigure.getUsername(), dataTransferConfigure.getPassword());
        } else if (ClientType.Cluster.equals(dataTransferConfigure.getClientType())) {
            connector = CanalConnectors.newClusterConnector(dataTransferConfigure.getCluster().getZkHosts(), dataTransferConfigure.getDestination(),
                    dataTransferConfigure.getUsername(), dataTransferConfigure.getPassword());
        }
        connector.connect();
        connector.subscribe(dataTransferConfigure.getSimple().getSubscribe());
        connector.rollback();
        return connector;
    }

    @Override
    public void processor() throws Throwable {
        Message message = canalConnector.getWithoutAck(dataTransferConfigure.getBatchSize());
        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
            Thread.sleep(dataTransferConfigure.getEmptySleepTime());
        } else {
            processor(message.getEntries());
            canalConnector.ack(batchId);
        }
    }
}