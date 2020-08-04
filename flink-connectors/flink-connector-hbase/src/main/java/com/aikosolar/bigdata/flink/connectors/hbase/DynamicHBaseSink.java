package com.aikosolar.bigdata.flink.connectors.hbase;

import com.aikosolar.bigdata.flink.connectors.hbase.constants.Constants;
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriter;
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * DynamicHBaseSink(WIP)
 * <p>
 * 缺点：单条写入
 *
 * @author carlc
 */
public class DynamicHBaseSink extends RichSinkFunction<Map<String, Object>> {
    private static final Logger L = LoggerFactory.getLogger(HBaseWriter.class);

    private Map<String/*topic*/, String/*hTableName*/> tableMapping;
    private Map<String/*topic*/, String/*family*/> familyMapping;

    private final HBaseWriterConfig writerConfig;
    private transient Connection connection;

    public DynamicHBaseSink(HBaseWriterConfig writerConfig, Map<String, String> tableMapping,
                            Map<String, String> familyMapping) {
        Preconditions.checkNotNull(writerConfig);
        Preconditions.checkNotNull(tableMapping);

        this.writerConfig = writerConfig;
        this.tableMapping = tableMapping;
        this.familyMapping = familyMapping == null ? new HashMap<>() : familyMapping;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        writerConfig.getHbaseConfig().forEach(conf::set);

        this.connection = ConnectionFactory.createConnection(conf);
    }

    @Override
    public void invoke(Map<String, Object> data, Context context) throws Exception {
        // S1: 获取Topic
        Object topic = data.remove(Constants.topic);
        if (topic == null || StringUtils.isBlank(topic.toString())) {
            return;
        }
        // S2: Topic转为HBase表名
        String hTableName = tableMapping.get(topic);
        if (StringUtils.isBlank(hTableName)) {
            return;
        }
        // 获取rowKey
        Object rowKey = data.remove(Constants.ROW_KEY);
        if (rowKey == null || StringUtils.isBlank(topic.toString())) {
            return;
        }
        // 获取列簇(默认:cf)
        String family = familyMapping.getOrDefault(topic,
                familyMapping.getOrDefault(Constants.DEFAULT_FAMILY_KEY,
                        Constants.DEFAULT_FAMILY_VALUE));
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(hTableName));
            Put put = new Put(Bytes.toBytes(rowKey.toString()));
            for (Map.Entry<String, Object> en : data.entrySet()) {
                String name = en.getKey();
                Object value = en.getValue();
                if (value != null) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(name), Bytes.toBytes(value.toString()));
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
        L.info("关闭HBase连接");
        IOUtils.closeQuietly(this.connection);
    }
}
