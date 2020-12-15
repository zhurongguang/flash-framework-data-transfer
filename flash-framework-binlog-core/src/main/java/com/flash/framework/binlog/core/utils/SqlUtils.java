package com.flash.framework.binlog.core.utils;

import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigDecimal;
import java.text.ParseException;

/**
 * @author zhurg
 * @date 2020/11/17 - 下午2:44
 */
@Slf4j
public class SqlUtils {

    private static final String[] DATETIME_FORMATTER_ARRAY = {"yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS"};

    public static Object parseObject(Integer sqlType, String value) {
        if (null != value && StringUtils.isNotEmpty(value)) {
            switch (sqlType) {
                case -7:
                case -5:
                    return Long.parseLong(value);
                case -6:
                case 4:
                case 5:
                    return Integer.parseInt(value);
                case 2:
                case 3:
                    return new BigDecimal(value);
                case 6:
                case 7:
                    return Float.parseFloat(value);
                case 8:
                    return Double.parseDouble(value);
                case 16:
                    return Boolean.parseBoolean(value);
                case 91:
                    try {
                        return DateUtils.parseDate(value, "yyyy-MM-dd");
                    } catch (ParseException e1) {
                        log.error("[Binlog] date {} parse failed,cause:{}", value, Throwables.getStackTraceAsString(e1));
                        return null;
                    }
                case 92:
                    try {
                        return DateUtils.parseDate(value, "HH:mm:ss", "HH:mm:ss.SSS");
                    } catch (ParseException e2) {
                        log.error("[Binlog] date {} parse failed,cause:{}", value, Throwables.getStackTraceAsString(e2));
                        return null;
                    }
                case 93:
                    try {
                        return DateUtils.parseDate(value, DATETIME_FORMATTER_ARRAY[index(value)]);
                    } catch (ParseException e3) {
                        log.error("[Binlog] date {} parse failed,cause:{}", value, Throwables.getStackTraceAsString(e3));
                        return null;
                    }
                default:
                    return value;
            }
        } else {
            return null;
        }
    }

    private static int index(String value) {
        int i = value.lastIndexOf(".");
        return -1 != i && value.length() - 1 != i ? 1 : 0;
    }
}