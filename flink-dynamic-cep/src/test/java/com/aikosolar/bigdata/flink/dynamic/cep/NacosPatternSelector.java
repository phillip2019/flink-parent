package com.aikosolar.bigdata.flink.dynamic.cep;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;

import java.util.Properties;
import java.util.concurrent.Executor;

/*

nacos 上的配置如下:

import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
import com.aikosolar.bigdata.flink.dynamic.cep.AviatorCondition
import org.apache.flink.cep.pattern.conditions.IterativeCondition

def getPattern() {
    return Pattern.<Tuple3<String, String, String>> begin("start")
            .where(new AviatorCondition("f2 == \"order\""))
            .next("next")
            .where(new AviatorCondition("f2 == \"pay\""))
            .within(Time.seconds(3))
}

*/

/**
 * 基于 <a href="https://nacos.io">Nacos</a> 的 Pattern 选择器
 *
 * @author carlc
 */
public class NacosPatternSelector extends DynamicPatternSelector<Tuple3<String, String, String>> implements Listener {

    private ConfigService configService;

    private Pattern<Tuple3<String, String, String>, ?> pattern;


    public NacosPatternSelector() {
        String serverAddr = "localhost";
        String dataId = "test";
        String group = "FLINK-EVE";
        Properties properties = new Properties();
        properties.put(PropertyKeyConst.SERVER_ADDR, serverAddr);
        try {
            configService = NacosFactory.createConfigService(properties);
            String content = configService.getConfig(dataId, group, 5000);
            System.out.println("服务器groovy代码:\n" + content);
            this.pattern = (Pattern<Tuple3<String, String, String>, ?>) ScriptEngine.getPattern(content, "getPattern");
            configService.addListener(dataId, group, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    @Override
    public Pair<String, Pattern<Tuple3<String, String, String>, ?>> select() throws Exception {
        return Pair.of("test", this.pattern);
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
    public void receiveConfigInfo(String content) {
        try {
            this.pattern = (Pattern<Tuple3<String, String, String>, ?>) ScriptEngine.getPattern(content, "getPattern");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Executor getExecutor() {
        return null;
    }
}
