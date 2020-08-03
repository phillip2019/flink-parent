package com.aikosolar.bigdata.flink.connectors.jdbc.connection;

import java.sql.Connection;

/**
 * @author carlc
 */
public interface JdbcConnectionProvider {
    Connection getConnection() throws Exception;

    Connection reestablishConnection() throws Exception;
}