package com.aikosolar.bigdata.flink.connectors.hbase;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * DynamicHBaseSink(WIP) 未完成
 * <p>
 * 缺点：单条写入
 *
 * @author carlc
 */
public class DynamicHBaseSink extends RichSinkFunction<Map<String, Object>> {

    private Map<String/*topic*/, String/*hTableName*/> tableMapping;

    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        ExecutorService threads = Executors.newFixedThreadPool(4);
        this.connection = ConnectionFactory.createConnection(conf, threads);
    }

    @Override
    public void invoke(Map<String, Object> data, Context context) throws Exception {
        // S1: 获取Topic
        Object topic = data.remove("_topic_");
        if (topic == null || StringUtils.isBlank(topic.toString())) {
            return;
        }
        // S2: Topic转为HBase表名
        String hTableName = tableMapping.get(topic);
        if (StringUtils.isBlank(hTableName)) {
            return;
        }
        // 获取rowKey
        Object rowKey = data.remove("_row_key_");
        if (rowKey == null || StringUtils.isBlank(topic.toString())) {
            return;
        }
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(hTableName));
            Put put = new Put(Bytes.toBytes(rowKey.toString()));
            // put.setDurability(d);
            for (Map.Entry<String, Object> en : data.entrySet()) {
                String name = en.getKey();
                Object value = en.getValue();
                if (value != null) {
                    put.addColumn(Bytes.toBytes("c1"), Bytes.toBytes(name), Bytes.toBytes(value.toString()));
                }
            }
            table.put(put);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    public void close() throws Exception {
    }
}
