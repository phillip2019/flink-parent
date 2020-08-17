package com.aikosolar.bigdata.flink.dynamic.cep;

import com.googlecode.aviator.AviatorEvaluator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AviatorCondition extends SimpleCondition<Tuple3<String, String, String>> implements Serializable {
    private String script;

    public AviatorCondition(String script) {
        this.script = script;
    }

    @Override
    public boolean filter(Tuple3<String, String, String> value) throws Exception {
        Map<String, Object> env = new HashMap<String, Object>();
        env.put("f0", value.f0);
        env.put("f1", value.f1);
        env.put("f2", value.f2);
        Boolean result = false;
        try {
            result = (Boolean) AviatorEvaluator.execute(script, env);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
