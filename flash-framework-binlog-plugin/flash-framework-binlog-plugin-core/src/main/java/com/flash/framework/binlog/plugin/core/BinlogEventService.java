package com.flash.framework.binlog.plugin.core;

import com.flash.framework.binlog.common.event.DataEvent;
import com.flash.framework.binlog.common.event.EventType;
import com.flash.framework.binlog.plugin.core.annotation.BinlogEvent;
import com.flash.framework.binlog.plugin.core.handler.BinlogEventHandler;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

/**
 * @author zhurg
 * @date 2020/12/8 - 上午11:48
 */
@Slf4j
@Component
public class BinlogEventService implements ApplicationContextAware {

    private Table<String, EventType, BinlogEventHandler> binlogEventHandlerTable = HashBasedTable.create();

    /**
     * 处理binlog事件
     *
     * @param dataEvent
     */
    public void onBinlogEvent(DataEvent dataEvent) {
        BinlogEventHandler handler = binlogEventHandlerTable.get(String.format("%s.%s", dataEvent.getSchema(), dataEvent.getTable()), dataEvent.getEventType());
        if (Objects.isNull(handler)) {
            log.warn("can not fund handler for database {} table {} eventType {}", dataEvent.getSchema(), dataEvent.getTable(), dataEvent.getEventType());
        } else {
            handler.onEvent(dataEvent);
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, BinlogEventHandler> beans = applicationContext.getBeansOfType(BinlogEventHandler.class);
        if (MapUtils.isNotEmpty(beans)) {
            beans.values().forEach(bean -> {
                BinlogEvent anno;
                if (AopUtils.isAopProxy(bean)) {
                    anno = AnnotationUtils.findAnnotation(AopUtils.getTargetClass(bean), BinlogEvent.class);
                } else {
                    anno = AnnotationUtils.findAnnotation(bean.getClass(), BinlogEvent.class);
                }
                if (Objects.isNull(anno)) {
                    throw new RuntimeException("BinlogEventHandler " + bean.getClass() + " has not annotated by @BinlogEvent");
                }
                for (String table : anno.tables()) {
                    for (EventType eventType : anno.eventTypes()) {
                        binlogEventHandlerTable.put(String.format("%s.%s", anno.database(), table), eventType, bean);
                    }
                }
            });
        }
    }
}