package com.aikosolar.bigdata.flink.table;

import com.alibaba.fastjson.JSON;

import java.util.Map;

public class Utils {
    public static AlarmBean toJson(Map<String,Object> map){
        return JSON.parseObject(JSON.toJSONString(map),AlarmBean.class);
    }

}
