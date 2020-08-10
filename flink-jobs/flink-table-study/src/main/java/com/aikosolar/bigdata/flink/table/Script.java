package com.aikosolar.bigdata.flink.table;

/**
 * @author carlc
 */
public interface Script extends Comparable<Script> {
    String getScriptType();

    String getScript();

    int order();
}
