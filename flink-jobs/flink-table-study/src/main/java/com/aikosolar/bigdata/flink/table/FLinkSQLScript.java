package com.aikosolar.bigdata.flink.table;

/**
 * @author carlc
 */
public class FLinkSQLScript implements FLinkSQL {

    private static final String SCRIPT_TYPE = "flink-sql";

    private String script;
    private String sqlType;
    private int order;

    public FLinkSQLScript(String script, String sqlType, int order) {
        this.script = script;
        this.sqlType = sqlType;
        this.order = order;
    }

    @Override
    public String getScriptType() {
        return SCRIPT_TYPE;
    }

    @Override
    public String getSqlType() {
        return this.sqlType;
    }

    @Override
    public int order() {
        return this.order;
    }


    public String getScript() {
        return this.script;
    }

    @Override
    public int compareTo(Script o) {
        return this.order() - o.order();
    }

}
