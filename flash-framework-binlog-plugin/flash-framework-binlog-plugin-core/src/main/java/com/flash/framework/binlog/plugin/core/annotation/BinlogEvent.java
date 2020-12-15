package com.flash.framework.binlog.plugin.core.annotation;

import com.flash.framework.binlog.common.event.EventType;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * @author zhurg
 * @date 2020/12/8 - 上午11:43
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface BinlogEvent {

    /**
     * database
     *
     * @return
     */
    String database();

    /**
     * tables
     *
     * @return
     */
    String[] tables();

    /**
     * EventType
     *
     * @return
     */
    EventType[] eventTypes();
}