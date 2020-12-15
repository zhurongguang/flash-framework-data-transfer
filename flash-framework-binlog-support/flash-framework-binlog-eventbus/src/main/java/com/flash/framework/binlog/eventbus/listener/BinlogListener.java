package com.flash.framework.binlog.eventbus.listener;

import java.lang.annotation.*;

/**
 * @author zhurg
 * @date 2020/11/18 - 下午6:29
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface BinlogListener {

    String schema();

    String table();
}