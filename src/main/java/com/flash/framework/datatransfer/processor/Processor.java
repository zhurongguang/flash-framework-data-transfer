package com.flash.framework.datatransfer.processor;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * @author zhurg
 * @date 2019/4/26 - 下午3:14
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface Processor {

    /**
     * 数据库
     *
     * @return
     */
    String schema();

    /**
     * 表
     *
     * @return
     */
    String table();

    /**
     * 监听事件
     *
     * @return
     */
    CanalEntry.EventType eventType();

    /**
     * 排序值
     *
     * @return
     */
    int sort() default 0;
}