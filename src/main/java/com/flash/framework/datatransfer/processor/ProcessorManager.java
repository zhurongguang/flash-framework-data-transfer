package com.flash.framework.datatransfer.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author zhurg
 * @date 2019/4/17 - 下午3:02
 */
@Slf4j
public class ProcessorManager implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    private Table<String, CanalEntry.EventType, List<DataProcessor>> processors = HashBasedTable.create();


    @PostConstruct
    public void init() {
        Map<String, DataProcessor> processorBeans = applicationContext.getBeansOfType(DataProcessor.class);
        if (!CollectionUtils.isEmpty(processorBeans)) {
            processorBeans.forEach((name, processor) -> {
                Processor annon = AnnotationUtils.findAnnotation(processor.getClass(), Processor.class);
                if (null == annon) {
                    throw new IllegalArgumentException("[DataTransfer] DataProcessor " + processor.getClass().getSimpleName() + " must set annotation @Processor");
                }
                processor.setSort(annon.sort());
                String rowKey = annon.schema() + "." + annon.table();
                if (!processors.contains(rowKey, annon.eventType())) {
                    processors.put(rowKey, annon.eventType(), Lists.newArrayList(processor));
                } else {
                    processors.get(rowKey, annon.eventType()).add(processor);
                }
            });
        }
    }

    /**
     * 处理业务
     *
     * @param rowKey
     * @param eventType
     * @param data
     */
    public void processor(String rowKey, CanalEntry.EventType eventType, String data) {
        if (processors.contains(rowKey, eventType)) {
            List<DataProcessor> processorList = processors.get(rowKey, eventType);
            Collections.sort(processorList);
            processorList.forEach(p -> {
                Type type = p.getClass().getGenericSuperclass();
                Type[] genericType = ((ParameterizedType) type).getActualTypeArguments();
                Class clazz = (Class) genericType[0];
                p.process(JSON.parseObject(data, clazz));
            });
        } else {
            log.warn("[DataTransfer] can not fund DataProcessor for schema.table {} eventType {}", rowKey, eventType.name());
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}