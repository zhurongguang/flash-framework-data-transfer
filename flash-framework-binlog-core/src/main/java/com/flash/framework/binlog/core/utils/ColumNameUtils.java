package com.flash.framework.binlog.core.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zhurg
 * @date 2019/4/17 - 下午2:48
 */
public class ColumNameUtils {

    private static Pattern linePattern = Pattern.compile("_(\\w)");

    /**
     * 下划线转驼峰
     *
     * @param columName
     * @return
     */
    public static String lineToHump(String columName) {
        columName = columName.toLowerCase();
        Matcher matcher = linePattern.matcher(columName);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, matcher.group(1).toUpperCase());
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}