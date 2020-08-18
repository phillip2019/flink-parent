package com.aikosolar.bigdata.flink.connectors.hbase.utils;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author carlc
 */
public class Puts {

    public static void addColumn(Put put, String key, Object value) {
        if (value != null) {
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(key), Bytes.toBytes(value.toString()));
        }
    }
}
