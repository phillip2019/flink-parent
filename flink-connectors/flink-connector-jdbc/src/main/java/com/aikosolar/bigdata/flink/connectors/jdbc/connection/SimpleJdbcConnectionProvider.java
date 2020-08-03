package com.aikosolar.bigdata.flink.connectors.jdbc.connection;


import com.aikosolar.bigdata.flink.connectors.jdbc.conf.JdbcConnectionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author carlc
 */
public class SimpleJdbcConnectionProvider implements JdbcConnectionProvider, Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SimpleJdbcConnectionProvider.class);

    private final JdbcConnectionOptions jdbcOptions;

    private transient volatile Connection connection;

    public SimpleJdbcConnectionProvider(JdbcConnectionOptions jdbcOptions) {
        this.jdbcOptions = jdbcOptions;
    }

    @Override
    public Connection getConnection() throws SQLException, ClassNotFoundException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    Class.forName(jdbcOptions.getDriverName());
                    connection = DriverManager.getConnection(jdbcOptions.getUrl(), jdbcOptions.getUsername(), jdbcOptions.getPassword());
                }
            }
        }
        return connection;
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.info("JDBC connection close failed.", e);
        } finally {
            connection = null;
        }
        connection = getConnection();
        return connection;
    }
}
