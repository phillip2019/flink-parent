package com.aikosolar.bigdata.flink.job.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GPUtils {

    public static final String GP_TYPE_KEY = "GP_TYPE";
    public static final String GP_TYPE1 = "GP1";
    public static final String GP_TYPE2 = "GP2";

    public static String getString(Object json, String jsonPath, String defaultValue) {
        Object v = JSONPath.eval(json, jsonPath);
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof String) {
            return (String) v;
        }
        return null;
    }


    public static Map<String, Object> parse(String json) {
        Map<String, Object> map = new HashMap<>();
        JSONObject jsonObject = JSON.parseObject(json);
        String equipmentID = jsonObject.getString("EquipmentID");
        JSONObject data = jsonObject.getJSONObject("Data");
        if (!data.containsKey("MtlEvtData") && !data.containsKey("EqState")) { //gp1
            return null;
        }
        map.put("EquipmentID", equipmentID);
        if (data.containsKey("MtlEvtData")) { //gp1
            map.put(GP_TYPE_KEY, GP_TYPE1);
            JSONObject eqState = data.getJSONObject("MtlEvtData");
            if (eqState != null) {
                JSONObject JobMd = eqState.getJSONObject("JobMd");

                if (JobMd != null) {
                    String ImageProcessingTime_ms = JobMd.getString("@ImageProcessingTime_ms");
                    String ClassificationProcessingTime_ms = JobMd.getString("@ClassificationProcessingTime_ms");
                    map.put("ImageProcessingTime_ms", ImageProcessingTime_ms);
                    map.put("ClassificationProcessingTime_ms", ClassificationProcessingTime_ms);

                }
                JSONObject MtlEvt = eqState.getJSONObject("MtlEvt");
                if (MtlEvt != null) {
                    String name = MtlEvt.getString("@Name");
                    map.put("Name", name);
                    JSONObject MtlList = MtlEvt.getJSONObject("MtlList");
                    if (MtlList != null) {
                        JSONObject Mtl = MtlList.getJSONObject("Mtl");
                        if (Mtl != null) {
                            String EqMtlId = Mtl.getString("@EqMtlId");
                            String Come = Mtl.getString("@Come");
                            String ToolId = getString(Mtl, "$.MeasValsList.MeasVals.\\@ToolId", null);
                            map.put("EqMtlId", EqMtlId);
                            map.put("Come", Come);
                            map.put("ToolId", ToolId);
                            List<JSONObject> MeasVal = (List) JSONPath.eval(Mtl, "$.MeasValsList.MeasVals.MeasVal");
                            if (MeasVal != null) {
                                for (JSONObject o : MeasVal) {
                                    map.put(o.get("@Id").toString(), o.get("#text").toString().replace("\\n", "").trim());
                                }
                            }
                        }
                    }
                }
            }
        } else {
            map.put(GP_TYPE_KEY, GP_TYPE2);
            JSONObject eqState = data.getJSONObject("EqState");
            if (eqState != null) {
                JSONObject eq = eqState.getJSONObject("Eq");
                if (eq != null) {
                    String EqId = eq.getString("@EqId");
                    String Id = getString(eq, "$.States.State.\\@Id", null);
                    String Come = getString(eq, "$.States.State.\\@Come", null);
                    map.put("EquipmentID", equipmentID);
                    map.put("EqId", EqId);
                    map.put("StateId", Id);
                    map.put("Come", Come);
                }
            }
        }
        if (StringUtils.isBlank(MapUtils.getString(map, "EquipmentID", null)) || StringUtils.isBlank(MapUtils.getString(map, "Come", null))) {
            return null;
        }
        return map;
    }
}
