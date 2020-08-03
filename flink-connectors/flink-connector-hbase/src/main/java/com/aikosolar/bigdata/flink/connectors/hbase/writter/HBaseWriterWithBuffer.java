package com.aikosolar.bigdata.flink.connectors.hbase.writter;

import com.aikosolar.bigdata.flink.connectors.hbase.HBaseOperation;
import com.aikosolar.bigdata.flink.connectors.hbase.mapper.HBaseMutationConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@SuppressWarnings("all")
public class HBaseWriterWithBuffer<IN> implements IHBaseWriter<IN> {
    private static final Logger L = LoggerFactory.getLogger(HBaseWriter.class);

    private final TableName tableName;
    private final HBaseMutationConverter<IN> mutationConverter;
    private transient BufferedMutator hbaseTable;
    private transient Connection connection;
    private transient HBaseWriterConfig writerConfig;
    private transient ScheduledExecutorService schedule;

    private final LongAdder counter;

    public HBaseWriterWithBuffer(HBaseWriterConfig writerConfig, String table, HBaseMutationConverter<IN> mutationConverter) throws Exception {
        this.tableName = TableName.valueOf(table);
        this.mutationConverter = mutationConverter;
        this.writerConfig = writerConfig;
        this.counter = new LongAdder();

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        writerConfig.getHbaseConfig().forEach(conf::set);

        ExecutorService threads = Executors.newFixedThreadPool(4);
        this.connection = ConnectionFactory.createConnection(conf, threads);

        BufferedMutatorParams params = new BufferedMutatorParams(this.tableName);
        params.writeBufferSize(writerConfig.getWriteBufferSize());
        hbaseTable = this.connection.getBufferedMutator(params);
        schedule = Executors.newScheduledThreadPool(1);
        schedule.scheduleAtFixedRate(() -> {
            try {
                flush();
            } catch (IOException e) {
                // todo 异常处理
                L.error("定时刷新缓存失败", e);
            }

        }, writerConfig.getAsyncFlushInterval(), writerConfig.getAsyncFlushInterval(), TimeUnit.SECONDS);
    }

    public void init() {
        // todo ...
    }

    public void write(IN record, HBaseOperation operation) throws IOException {
        Mutation mutation = mutationConverter.convertToMutation(operation, record);
        Durability d = null;
        if(StringUtils.isNotBlank(writerConfig.getDurability())){
            try {
                d =  Durability.valueOf(writerConfig.getDurability());
                mutation.setDurability(d);
            }catch (Exception e){
            }
        }
        switch (operation) {
            case INSERT:
                hbaseTable.mutate(mutation);
                counter.add(1L);
                break;
            case DELETE:
                hbaseTable.mutate(mutation);
                counter.add(1L);
                break;
            default:
                break;
        }
        if (counter.longValue() > 0 && counter.longValue() % writerConfig.getAsyncFlushSize() == 0) {
            this.flush();
        }
    }

    public void flush() throws IOException {
        this.hbaseTable.flush();
    }

    @Override
    public void close() throws Exception {
        L.info("关闭HBase连接");
        if (this.hbaseTable != null) {
            this.hbaseTable.close();
        }
        if (this.connection != null) {
            connection.close();
        }
        try {
            schedule.shutdown();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
