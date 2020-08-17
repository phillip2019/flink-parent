package com.aikosolar.bigdata.flink.dynamic.cep;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * 基于 <a href="https://nacos.io">Nacos</a> 的 Pattern 选择器
 *
 * @author carlc
 */
public class NacosPatternSelector extends DynamicPatternSelector<String> implements Listener {

    private ConfigService configService;

    private String content;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String serverAddr = "localhost";
        String dataId = "flink-eve-prod";
        String group = "DEFAULT_GROUP";
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        configService = NacosFactory.createConfigService(properties);
        this.content = configService.getConfig(dataId, group, 5000);
        configService.addListener(dataId, group, this);
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
        if (configService != null) {
            configService.shutDown();
        }
    }

    @Override
    public void receiveConfigInfo(String configInfo) {
        this.content = configInfo;
    }

    @Override
    public Executor getExecutor() {
        return null;
    }
}
