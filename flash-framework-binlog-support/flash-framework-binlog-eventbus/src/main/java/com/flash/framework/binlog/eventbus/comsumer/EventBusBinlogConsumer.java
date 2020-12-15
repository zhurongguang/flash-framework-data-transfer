package com.flash.framework.binlog.eventbus.comsumer;

import com.flash.framework.binlog.common.event.DataEvent;
import com.flash.framework.binlog.eventbus.listener.BinlogEventListener;
import com.flash.framework.binlog.eventbus.listener.BinlogListener;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Objects;

/**
 * @author zhurg
 * @date 2020/11/18 - 下午6:25
 */
@Slf4j
@Component
public class EventBusBinlogConsumer implements ApplicationContextAware {

    @Autowired
    private EventBus eventBus;

    private Table<String, String, BinlogEventListener> binlogListenerTable = HashBasedTable.create();

    @PostConstruct
    public void init() {
        eventBus.register(this);
    }

    @Subscribe
    @AllowConcurrentEvents
    public void onEvent(DataEvent event) {
        if (binlogListenerTable.contains(event.getSchema(), event.getTable())) {
            binlogListenerTable.get(event.getSchema(), event.getTable()).onEvent(event);
        } else {
            log.warn("[Binlog] did not has any BinlogEventListener for schema {} table {}", event.getSchema(), event.getTable());
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, BinlogEventListener> beans = applicationContext.getBeansOfType(BinlogEventListener.class);
        if (MapUtils.isNotEmpty(beans)) {
            beans.values().forEach(bean -> {
                BinlogListener anno;
                if (AopUtils.isAopProxy(bean)) {
                    anno = AnnotationUtils.findAnnotation(AopUtils.getTargetClass(bean), BinlogListener.class);
                } else {
                    anno = AnnotationUtils.findAnnotation(bean.getClass(), BinlogListener.class);
                }

                if (Objects.isNull(anno)) {
                    log.warn("[Binlog] BinlogEventListener {} did not has annotation @BinlogListener", bean);
                    return;
                }
                binlogListenerTable.put(anno.schema(), anno.table(), bean);
            });
        }
    }
}
