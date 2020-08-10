package com.aikosolar.bigdata.flink.table;

/**
 * @author carlc
 */
public class FLinkSettingScript implements Script {

    private static final String SCRIPT_TYPE = "flink-settings";

    @Override
    public String getScriptType() {
        return SCRIPT_TYPE;
    }

    @Override
    public String getScript() {
        return "k1=v1";
    }

    @Override
    public int compareTo(Script o) {
        return 0;
    }

    @Override
    public int order() {
        return 0;
    }
}
