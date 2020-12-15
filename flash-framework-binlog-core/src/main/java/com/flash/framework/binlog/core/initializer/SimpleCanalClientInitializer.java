package com.flash.framework.binlog.core.initializer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.flash.framework.binlog.common.BinlogConstants;
import com.flash.framework.binlog.core.autoconfigure.CanalConfigure;
import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.net.InetSocketAddress;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午5:17
 */
@Slf4j
public class SimpleCanalClientInitializer extends CanalClientInitializer {


    public SimpleCanalClientInitializer(CanalConfigure canalConfigure) {
        super(canalConfigure);
    }

    @Override
    public void checkConfigure() throws IllegalArgumentException {
        Assert.notNull(canalConfigure.getAddress(), "[Binlog] properties binlog.address can not be null");
        Assert.notNull(canalConfigure.getFilterRegex(), "[Binlog] properties binlog.filter-regex can not be null");
        if (canalConfigure.getBatchSize() <= 0) {
            canalConfigure.setBatchSize(1024);
        }
    }

    @Override
    public CanalConnector initClient() {
        CanalConnector connector;
        //集群模式
        if (canalConfigure.getAddress().startsWith(BinlogConstants.CLUSTER_ADDRESS)) {
            connector = CanalConnectors.newClusterConnector(canalConfigure.getAddress().replaceAll(BinlogConstants.CLUSTER_ADDRESS, ""), canalConfigure.getDestination(),
                    canalConfigure.getUsername(), canalConfigure.getPassword());
        } else {
            String[] host = canalConfigure.getAddress().split(":");
            if (host.length != 2) {
                throw new IllegalArgumentException("[Binlog] properties binlog.address illegal");
            }
            connector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(host[0], Integer.parseInt(host[1])), canalConfigure.getDestination(),
                    canalConfigure.getUsername(), canalConfigure.getPassword());
        }
        connector.connect();
        connector.subscribe(canalConfigure.getFilterRegex());
        connector.rollback();
        return connector;
    }

    @Override
    public void processor() throws Throwable {
        Message message = canalConnector.getWithoutAck(canalConfigure.getBatchSize());
        long batchId = message.getId();
        int size = message.getEntries().size();
        if (batchId == -1 || size == 0) {
            Thread.sleep(canalConfigure.getIdelSleepTime());
            canalConnector.ack(batchId);
        } else {
            try {
                process(message.getEntries());
                canalConnector.ack(batchId);
            } catch (Exception e) {
                log.error("[Binlog] canal data processor failed,cause:{}", Throwables.getStackTraceAsString(e));
                canalConnector.rollback(batchId);
            }
        }
    }
}