package com.flash.framework.datatransfer.initializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.flash.framework.datatransfer.autoconfigure.DataTransferConfigure;
import com.flash.framework.datatransfer.processor.ProcessorManager;
import com.flash.framework.datatransfer.utils.ColumNameUtils;
import com.github.rholder.retry.*;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午5:14
 */
@Slf4j
public abstract class CanalClientInitializer implements InitializingBean, DisposableBean {

    protected final DataTransferConfigure dataTransferConfigure;

    private volatile boolean running = false;

    protected CanalConnector canalConnector;

    private Thread processorThread;

    private static final String JSON_COLUMN_TYPE = "json";

    @Autowired
    private ProcessorManager processorManager;

    public CanalClientInitializer(DataTransferConfigure dataTransferConfigure) {
        this.dataTransferConfigure = dataTransferConfigure;
    }

    /**
     * 校验配置
     *
     * @throws IllegalArgumentException
     */
    public abstract void checkConfigure() throws IllegalArgumentException;

    /**
     * 初始化
     */
    public abstract CanalConnector initClient();

    /**
     * 处理业务
     *
     * @throws Throwable
     */
    public abstract void processor() throws Throwable;

    @Override
    public void afterPropertiesSet() {
        checkConfigure();

        processorThread = new Thread(new ThreadGroup("DataTransfer-Thread"), () -> {
            doInit();
            while (running) {
                try {
                    processor();
                } catch (Throwable e) {
                    log.error("[DataTransfer] data processor failed ", e);
                    if (e instanceof CanalClientException) {
                        log.error("[DataTransfer] client connect to server failed");
                        destroy();
                        doInit();
                    }
                }
            }
        });

        processorThread.start();
    }

    /**
     * 销毁Client
     */
    @Override
    public void destroy() {
        running = false;

        if (null != canalConnector) {
            canalConnector.disconnect();
            log.info("[DataTransfer] client disconnected");
        }
    }

    /**
     * 业务处理
     *
     * @param entrys
     */
    protected void processor(List<CanalEntry.Entry> entrys) throws Throwable {
        for (CanalEntry.Entry entry : entrys) {

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String schemaName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();
            CanalEntry.EventType eventType = rowChange.getEventType();
            List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDataList) {
                String jsonData;
                switch (eventType) {
                    case DELETE:
                        jsonData = convert2Json(rowData.getBeforeColumnsList());
                        if (log.isDebugEnabled()) {
                            log.debug("[DataTransfer] received data {}, schema {} table {} eventType {}", jsonData, schemaName, tableName, eventType.name());
                        }
                        processorManager.processor(schemaName + "." + tableName, eventType, jsonData);
                        break;
                    case UPDATE:
                    case INSERT:
                        jsonData = convert2Json(rowData.getAfterColumnsList());
                        if (log.isDebugEnabled()) {
                            log.debug("[DataTransfer] received data {}, schema {} table {} eventType {}", jsonData, schemaName, tableName, eventType.name());
                        }
                        processorManager.processor(schemaName + "." + tableName, eventType, jsonData);
                        break;
                    default:
                        log.info("[DataTransfer] received data , schema {} table {} eventType {}", schemaName, tableName, eventType.name());
                        break;
                }
            }
        }
    }

    /**
     * 业务处理
     *
     * @param message
     * @throws Throwable
     */
    protected void processor(FlatMessage message) throws Throwable {
        List<Map<String, String>> columns = message.getData();
        Map<String, String> types = message.getMysqlType();
        CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(message.getType());
        columns.forEach(rowData -> {
            String jsonData;
            switch (eventType) {
                case DELETE:
                    jsonData = convert2Json(rowData, types);
                    if (log.isDebugEnabled()) {
                        log.debug("[DataTransfer] received data {}, schema {} table {} eventType {}", jsonData, message.getDatabase(), message.getTable(), eventType);
                    }
                    processorManager.processor(message.getDatabase() + "." + message.getTable(), CanalEntry.EventType.DELETE, jsonData);
                    break;
                case UPDATE:
                case INSERT:
                    jsonData = convert2Json(rowData, types);
                    if (log.isDebugEnabled()) {
                        log.debug("[DataTransfer] received data {}, schema {} table {} eventType {}", jsonData, message.getDatabase(), message.getTable(), eventType);
                    }
                    processorManager.processor(message.getDatabase() + "." + message.getTable(), CanalEntry.EventType.DELETE, jsonData);
                    break;
                default:
                    log.info("[DataTransfer] received data , schema {} table {} eventType {}", message.getDatabase(), message.getTable(), eventType);
                    break;
            }
        });

    }

    /**
     * 将sql的数据转换成json
     *
     * @param columns
     * @param types
     * @return
     */
    private String convert2Json(Map<String, String> columns, Map<String, String> types) {
        Map<String, Object> jsonMap = Maps.newHashMap();
        columns.forEach((columnName, value) -> {
            if (types.get(columnName).equalsIgnoreCase(JSON_COLUMN_TYPE)) {
                jsonMap.put(ColumNameUtils.lineToHump(columnName), JSON.parseObject(value));
            } else {
                jsonMap.put(ColumNameUtils.lineToHump(columnName), value);
            }
        });
        return JSON.toJSONString(jsonMap, SerializerFeature.BrowserCompatible);
    }

    /**
     * 将sql的数据转换成json
     *
     * @param columns
     * @return
     */
    private String convert2Json(List<CanalEntry.Column> columns) {
        Map<String, Object> jsonMap = Maps.newHashMap();
        columns.forEach(column -> {
            String columName = column.getName();
            if (column.getMysqlType().equalsIgnoreCase(JSON_COLUMN_TYPE)) {
                jsonMap.put(ColumNameUtils.lineToHump(columName), JSON.parseObject(column.getValue()));
            } else {
                jsonMap.put(ColumNameUtils.lineToHump(columName), column.getValue());
            }
        });
        return JSON.toJSONString(jsonMap, SerializerFeature.BrowserCompatible);
    }

    /**
     * 初始化
     *
     * @throws RuntimeException
     */
    private void doInit() throws RuntimeException {
        try {
            RetryerBuilder.<Boolean>newBuilder()
                    .retryIfException()
                    .retryIfRuntimeException()
                    .withWaitStrategy(WaitStrategies.incrementingWait(dataTransferConfigure.getRetryIntervalTime(), TimeUnit.MILLISECONDS, dataTransferConfigure.getRetryIntervalTime(), TimeUnit.MILLISECONDS))
                    .withStopStrategy(StopStrategies.stopAfterAttempt(dataTransferConfigure.getRetryTimes()))
                    .withRetryListener(new RetryListener() {
                        @Override
                        public <V> void onRetry(Attempt<V> attempt) {
                            if (attempt.hasException()) {
                                log.error("[DataTransfer] client connected failed,cause:" + attempt.getExceptionCause());
                            }
                        }
                    })
                    .build()
                    .call(() -> {
                        if (!running) {
                            canalConnector = initClient();
                            running = true;
                            log.info("[DataTransfer] client connected");
                        }
                        return Boolean.TRUE;
                    });
        } catch (Exception e) {
            log.error("[DataTransfer] client connected failed,cause:", e);
        }
    }
}