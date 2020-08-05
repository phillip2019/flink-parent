package com.aikosolar.bigdata.flink.connectors.hbase;

import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig;
import com.google.common.base.Joiner;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * HBase sink(根据特殊字段进行)
 *
 * @author carlc
 */
public class SimpleHBaseTableSink extends RichSinkFunction<Map<String, Object>> {

    private final String tableName;
    private final HBaseWriterConfig writerConfig;

    private transient TableName tName;
    private transient Connection connection;

    public SimpleHBaseTableSink(HBaseWriterConfig writerConfig, String tableName) throws Exception {
        Preconditions.checkNotNull(writerConfig);
        Preconditions.checkNotNull(tableName);

        this.writerConfig = writerConfig;
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.tName = TableName.valueOf(tableName);

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        this.writerConfig.getHbaseConfig().forEach(conf::set);
        this.connection = ConnectionFactory.createConnection(conf);
    }

    @Override
    public void invoke(Map<String, Object> data, Context context) throws Exception {
//        System.out.println(Joiner.on("#").withKeyValueSeparator("=").join(data));
        Table table = null;
        try {
            table = connection.getTable(this.tName);
            Put put = new Put(Bytes.toBytes(data.remove("row_key").toString()));
            for (Map.Entry<String, Object> en : data.entrySet()) {
                if (en.getValue() != null) {
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(en.getKey()), Bytes.toBytes(en.getValue().toString()));
                }
            }
            if (StringUtils.isNotBlank(writerConfig.getDurability())) {
                try {
                    Durability d = Durability.valueOf(writerConfig.getDurability());
                    put.setDurability(d);
                } catch (Exception e) {
                }
            }
            table.put(put);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    public void close() throws Exception {
        if(this.connection != null) {
            this.connection.close();
            this.connection = null;
        }
    }
}
