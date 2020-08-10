package com.aikosolar.bigdata.flink.table;

/**
 * @author carlc
 */
public interface Setting {
    String key();
    Object value();
    String valueType();
    Object defaultValue();
}
