package com.aikosolar.bigdata.flink.dynamic.cep;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;

/**
 * 动态Pattern选择器demo
 * 如何作到动态?
 * => 通知(mq/zk) + 轮询(db/redis)
 *
 * @author carlc
 */
public class DynamicPatternSelectorDemo extends DynamicPatternSelector<String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public Pair<String, Pattern<String, ?>> select() throws Exception {
        return null;
    }

    @Override
    public long getPeriod() {
        return 0;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
