package com.aikosolar.bigdata.flink.connectors.hbase.writter;

import com.aikosolar.bigdata.flink.connectors.hbase.HBaseOperation;
import com.aikosolar.bigdata.flink.connectors.hbase.mapper.HBaseMutationConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author carlc
 */
public class HBaseWriter<IN> implements IHBaseWriter<IN> {
    private static final Logger L = LoggerFactory.getLogger(HBaseWriter.class);

    private final TableName tableName;
    private final HBaseMutationConverter<IN> mutationConverter;
    private transient HBaseWriterConfig writerConfig;

    private transient Connection connection;

    public HBaseWriter(HBaseWriterConfig writerConfig, String table, HBaseMutationConverter<IN> mutationConverter) throws Exception {
        this.tableName = TableName.valueOf(table);
        this.mutationConverter = mutationConverter;
        this.writerConfig = writerConfig;

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        writerConfig.getHbaseConfig().forEach(conf::set);

        this.connection = ConnectionFactory.createConnection(conf);
    }

    @Override
    public void init() {

    }

    public void write(IN record, HBaseOperation operation) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(this.tableName);
            Mutation mutation = mutationConverter.convertToMutation(operation, record);
            if (StringUtils.isNotBlank(writerConfig.getDurability())) {
                try {
                    Durability d = Durability.valueOf(writerConfig.getDurability());
                    mutation.setDurability(d);
                } catch (Exception e) {
                }
            }
            switch (operation) {
                case INSERT:
                    table.put((Put) mutation);
                    break;
                case DELETE:
                    table.delete((Delete) mutation);
                    break;
                default:
                    break;
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    public void flush() {

    }

    @Override
    public void close() throws Exception {
        L.info("关闭HBase连接");
        IOUtils.closeQuietly(this.connection);
    }
}
