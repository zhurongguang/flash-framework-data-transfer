package com.flash.framework.datatransfer.processor;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.flash.framework.datatransfer.model.User;

/**
 * @author
 * @date 2019/4/25 - 下午2:22
 */
@Processor(schema = "test", table = "user", eventType = CanalEntry.EventType.DELETE)
public class TestUserDeleteDataProcessor extends DataProcessor<User> {

    @Override
    void process(User user) {
        System.out.println(user);
    }
}
