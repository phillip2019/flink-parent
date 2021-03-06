package com.aikosolar.bigdata.flink.connectors.jdbc.writter;

import org.apache.flink.util.function.BiConsumerWithException;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author carlc
 */
public interface JdbcWriter<T> extends BiConsumerWithException<PreparedStatement, T, SQLException>, Serializable {
    default void flush() {
    }

    default void update(PreparedStatement ps, T date) {

    }

    default boolean exists(PreparedStatement ps, T date) {
        throw new UnsupportedOperationException();
    }
}
