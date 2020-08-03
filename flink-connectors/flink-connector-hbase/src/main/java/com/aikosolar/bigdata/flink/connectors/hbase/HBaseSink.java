package com.aikosolar.bigdata.flink.connectors.hbase;

import com.aikosolar.bigdata.flink.connectors.hbase.mapper.HBaseMutationConverter;
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriter;
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterConfig;
import com.aikosolar.bigdata.flink.connectors.hbase.writter.HBaseWriterWithBuffer;
import com.aikosolar.bigdata.flink.connectors.hbase.writter.IHBaseWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author carlc
 */
public class HBaseSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger L = LoggerFactory.getLogger(HBaseSink.class);

    private final HBaseWriterConfig writerConfig;
    private final String hTableName;
    private final HBaseOperation operation;
    private final HBaseMutationConverter<IN> mutationConverter;

    private transient IHBaseWriter hBaseWriter;

    public HBaseSink(HBaseWriterConfig writerConfig, String hTableName, HBaseMutationConverter<IN> mutationConverter, HBaseOperation operation) throws Exception {
        Preconditions.checkNotNull(writerConfig);
        Preconditions.checkNotNull(hTableName);
        Preconditions.checkNotNull(mutationConverter);
        Preconditions.checkNotNull(operation);

        this.writerConfig = writerConfig;
        this.hTableName = hTableName;
        this.operation = operation;
        this.mutationConverter = mutationConverter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 这种不好额.
        if (!this.writerConfig.isAsync()) {
            this.hBaseWriter = new HBaseWriter(this.writerConfig, this.hTableName, this.mutationConverter);
        } else {
            this.hBaseWriter = new HBaseWriterWithBuffer(this.writerConfig, this.hTableName, this.mutationConverter);
        }
    }

    @Override
    public void invoke(IN in, Context context) throws Exception {
        hBaseWriter.write(in, this.operation);
    }

    @Override
    public void close() throws Exception {
        if (hBaseWriter != null) {
            try {
                hBaseWriter.flush();
            }finally {
                hBaseWriter.close();
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (hBaseWriter != null) {
            hBaseWriter.flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //no-op
    }
}

