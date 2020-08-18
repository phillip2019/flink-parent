package com.aikosolar.bigdata.flink.dynamic.cep;

import javax.script.Invocable;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class ScriptEngine {
    public static Object getPattern(String text, String name) throws ScriptException, NoSuchMethodException {
        ScriptEngineManager factory = new ScriptEngineManager();
        javax.script.ScriptEngine engine = factory.getEngineByName("groovy");
        assert engine != null;
        engine.eval(text);

        return ((Invocable) engine).invokeFunction(name);
    }

    public static void main(String[] args) throws Exception {
        String code = "import org.apache.flink.api.java.tuple.Tuple3\n" +
                "import org.apache.flink.cep.pattern.Pattern\n" +
                "import org.apache.flink.cep.pattern.conditions.IterativeCondition\n" +
                "import org.apache.flink.streaming.api.windowing.time.Time\n" +
                "\n" +
                "def getPattern() {\n" +
                "    return Pattern.<Tuple3<String, String, String>> begin(\"begin\").where(new IterativeCondition<Tuple3<String, String, String>>() {\n" +
                "        public boolean filter(Tuple3<String, String, String> value, IterativeCondition.Context<Tuple3<String, String, String>> ctx) throws Exception {\n" +
                "            return value.f2.equals(\"order\")\n" +
                "        }\n" +
                "    }).next(\"end\").where(new IterativeCondition<Tuple3<String, String, String>>() {\n" +
                "        public boolean filter(Tuple3<String, String, String> value, IterativeCondition.Context<Tuple3<String, String, String>> ctx) throws Exception {\n" +
                "            return value.f2.equals(\"pay\")\n" +
                "        }\n" +
                "    }).within(Time.seconds(3));\n" +
                "}";

        System.out.println(getPattern(code, "getPattern"));
    }
}