package com.flash.framework.binlog.core.initializer;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.flash.framework.binlog.common.event.DataEvent;
import com.flash.framework.binlog.common.event.EventType;
import com.flash.framework.binlog.common.event.RowEvent;
import com.flash.framework.binlog.core.autoconfigure.CanalConfigure;
import com.flash.framework.binlog.core.event.BinlogEventPublisher;
import com.flash.framework.binlog.core.utils.SqlUtils;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @author zhurg
 * @date 2019/4/16 - 下午5:14
 */
@Slf4j
public abstract class CanalClientInitializer implements InitializingBean, DisposableBean {

    protected final CanalConfigure canalConfigure;

    private volatile boolean running = false;

    private volatile boolean connected = false;

    protected CanalConnector canalConnector;

    private ExecutorService executorService;

    private ScheduledExecutorService scheduledExecutorService;

    @Autowired
    private BinlogEventPublisher binlogEventPublisher;

    public CanalClientInitializer(CanalConfigure canalConfigure) {
        this.canalConfigure = canalConfigure;
        this.executorService = new ThreadPoolExecutor(1, 1, 60L,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(1024), new ThreadFactoryBuilder().setNameFormat("binlog-thread-%d").build(), new ThreadPoolExecutor.AbortPolicy());
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("binlog-health-thread-%d").build());
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

        running = true;

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (!connected) {
                try {
                    connect();
                } catch (Throwable e) {
                    log.error("[Binlog] canal connect failed,cause:{}", Throwables.getStackTraceAsString(e));
                }
            }
        }, 0, canalConfigure.getHealthCheckTime(), TimeUnit.MILLISECONDS);

        executorService.execute(() -> {
            while (running) {
                try {
                    if (connected) {
                        processor();
                    } else {
                        Thread.sleep(canalConfigure.getHealthCheckTime());
                    }
                } catch (Throwable e) {
                    log.error("[Binlog] canal data pull failed,cause:{}", Throwables.getStackTraceAsString(e));
                    connected = false;
                }
            }
        });
    }

    /**
     * 销毁Client
     */
    @Override
    public void destroy() {
        connected = false;
        running = false;

        if (null != canalConnector) {
            try {
                if (canalConnector.checkValid()) {
                    canalConnector.disconnect();
                }
            } catch (CanalClientException e2) {
            }
        }

        executorService.shutdown();
        scheduledExecutorService.shutdown();

        log.info("[Binlog] data transfer client disconnected");
    }

    /**
     * 业务处理
     *
     * @param entrys
     */
    protected void process(List<CanalEntry.Entry> entrys) throws Throwable {
        for (CanalEntry.Entry entry : entrys) {

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            String schemaName = entry.getHeader().getSchemaName();
            String tableName = entry.getHeader().getTableName();

            if (filterTables(schemaName, tableName)) {
                log.debug("[Binlog] schema {} table {} was filter", schemaName, tableName);
                return;
            }

            CanalEntry.EventType eventType = rowChange.getEventType();
            List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
            DataEvent dataEvent = new DataEvent();
            dataEvent.setSchema(schemaName);
            dataEvent.setTable(tableName);

            switch (eventType) {
                case DELETE:
                    dataEvent.setEventType(EventType.DELETE);
                    break;
                case UPDATE:
                    dataEvent.setEventType(EventType.UPDATE);
                    break;
                case INSERT:
                    dataEvent.setEventType(EventType.CREATE);
                    break;
                default:
                    log.info("[Binlog] received data , schema {} table {} eventType {}", schemaName, tableName, eventType.name());
                    return;
            }

            if (Objects.nonNull(dataEvent.getEventType())) {
                dataEvent.setRows(toRowEvent(rowDataList));
                if (CollectionUtils.isNotEmpty(dataEvent.getRows())) {
                    binlogEventPublisher.publish(dataEvent);
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
    protected void process(FlatMessage message) throws Throwable {

        if (filterTables(message.getDatabase(), message.getTable())) {
            log.debug("[Binlog] schema {} table {} was filter", message.getDatabase(), message.getTable());
            return;
        }

        List<Map<String, String>> columns = message.getData();
        Map<String, Integer> sqlTypes = message.getSqlType();
        CanalEntry.EventType eventType = CanalEntry.EventType.valueOf(message.getType());
        DataEvent dataEvent = new DataEvent();
        dataEvent.setSchema(message.getDatabase());
        dataEvent.setTable(message.getTable());

        switch (eventType) {
            case DELETE:
                dataEvent.setEventType(EventType.DELETE);
                break;
            case UPDATE:
                dataEvent.setEventType(EventType.UPDATE);
                break;
            case INSERT:
                dataEvent.setEventType(EventType.CREATE);
                break;
            default:
                log.info("[Binlog] received data , schema {} table {} eventType {}", message.getDatabase(), message.getTable(), eventType.name());
                return;
        }

        if (Objects.nonNull(dataEvent.getEventType())) {
            dataEvent.setRows(toRowEvent(columns, sqlTypes, dataEvent.getEventType()));
            if (CollectionUtils.isNotEmpty(dataEvent.getRows())) {
                binlogEventPublisher.publish(dataEvent);
            }
        }

    }

    /**
     * filter tables
     *
     * @param schema
     * @param table
     * @return
     */
    private boolean filterTables(String schema, String table) {
        if (StringUtils.isBlank(canalConfigure.getFilterTables())) {
            return false;
        }
        List<String> filterTables = Splitter.on(",").splitToList(canalConfigure.getFilterTables());
        return filterTables.contains(String.format("%s.%s", schema, table));
    }

    /**
     * convert to RowEvent
     *
     * @param rowDatas
     * @return
     */
    private List<RowEvent> toRowEvent(List<CanalEntry.RowData> rowDatas) {
        return rowDatas.stream().map(rowData -> {
            RowEvent event = new RowEvent();
            if (CollectionUtils.isNotEmpty(rowData.getBeforeColumnsList())) {
                Map<String, Object> before = Maps.newHashMapWithExpectedSize(rowData.getBeforeColumnsList().size());
                rowData.getBeforeColumnsList().forEach(column ->
                        before.put(column.getName(), SqlUtils.parseObject(column.getSqlType(), column.getValue()))
                );
                event.setBefore(before);
            }
            if (CollectionUtils.isNotEmpty(rowData.getAfterColumnsList())) {
                Map<String, Object> after = Maps.newHashMapWithExpectedSize(rowData.getBeforeColumnsList().size());
                rowData.getAfterColumnsList().forEach(column ->
                        after.put(column.getName(), SqlUtils.parseObject(column.getSqlType(), column.getValue()))
                );
                event.setAfter(after);
            }
            return event;
        }).collect(Collectors.toList());
    }

    /**
     * convert to RowEvent
     *
     * @param columns
     * @param sqlTypes
     * @param eventType
     * @return
     */
    private List<RowEvent> toRowEvent(List<Map<String, String>> columns, Map<String, Integer> sqlTypes, EventType eventType) {
        switch (eventType) {
            case CREATE:
                RowEvent createEvent = new RowEvent();
                Map<String, Object> createAfter = Maps.newHashMapWithExpectedSize(columns.size());
                columns.get(0).forEach((k, v) -> createAfter.put(k, SqlUtils.parseObject(sqlTypes.get(k), v)));
                createEvent.setAfter(createAfter);
                return Lists.newArrayList(createEvent);
            case DELETE:
                RowEvent deleteEvent = new RowEvent();
                Map<String, Object> deleteBefore = Maps.newHashMapWithExpectedSize(columns.size());
                columns.get(0).forEach((k, v) -> deleteBefore.put(k, SqlUtils.parseObject(sqlTypes.get(k), v)));
                deleteEvent.setBefore(deleteBefore);
                return Lists.newArrayList(deleteEvent);
            case UPDATE:
                RowEvent updateEvent = new RowEvent();
                Map<String, Object> updateBefore = Maps.newHashMapWithExpectedSize(columns.size());
                Map<String, Object> updateAfter = Maps.newHashMapWithExpectedSize(columns.size());

                columns.get(0).forEach((k, v) -> updateAfter.put(k, SqlUtils.parseObject(sqlTypes.get(k), v)));
                columns.get(1).forEach((k, v) -> updateBefore.put(k, SqlUtils.parseObject(sqlTypes.get(k), v)));


                updateEvent.setBefore(updateBefore);
                updateEvent.setAfter(updateAfter);
                return Lists.newArrayList(updateEvent);
            default:
                return null;
        }
    }

    /**
     * connect canal
     */
    private void connect() {
        canalConnector = initClient();
        connected = true;
        log.info("[Binlog] client connected");
    }
}