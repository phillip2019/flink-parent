package com.aikosolar.bigdata.flink.connectors.hbase.mapper;

import com.aikosolar.bigdata.flink.connectors.hbase.HBaseOperation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.io.Serializable;

/**
 * @author carlc
 */
public interface HBaseMutationConverter<IN> extends Serializable {

    /**
     * 将 record 转换为 HBase {@link Mutation}. mutation 可以是 {@link Put} 或 {@link Delete}.
     */
    default Mutation convertToMutation(HBaseOperation operation, IN record) {
        switch (operation) {
            case INSERT:
                return insert(record);
            case DELETE:
                return delete(record);
        }
        return null;
    }

    Put insert(IN record);

    Delete delete(IN record);
}
