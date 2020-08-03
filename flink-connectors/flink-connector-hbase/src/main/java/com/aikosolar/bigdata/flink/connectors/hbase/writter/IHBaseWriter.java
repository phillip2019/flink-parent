package com.aikosolar.bigdata.flink.connectors.hbase.writter;

import com.aikosolar.bigdata.flink.connectors.hbase.HBaseOperation;

import java.io.IOException;

/**
 *
 * @author carlc
 */
public interface IHBaseWriter<IN> extends AutoCloseable {
    void init();

    void write(IN record, HBaseOperation operation) throws IOException;

    void flush() throws IOException;
}
