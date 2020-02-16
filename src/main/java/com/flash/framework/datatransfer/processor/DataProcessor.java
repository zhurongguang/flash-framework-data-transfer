package com.flash.framework.datatransfer.processor;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

/**
 * 数据处理器
 *
 * @author zhurg
 * @date 2019/4/17 - 下午2:58
 */
@Data
public abstract class DataProcessor<T> implements Comparable<DataProcessor<T>> {

    private int sort;

    abstract void process(T t);

    @Override
    public int compareTo(@NotNull DataProcessor<T> o) {
        return getSort() - o.getSort();
    }
}