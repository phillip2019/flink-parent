package com.aikosolar.bigdata.flink.connectors.jdbc;

import com.aikosolar.bigdata.flink.common.utils.IOUtils;
import com.aikosolar.bigdata.flink.connectors.jdbc.conf.JdbcConnectionOptions;
import com.aikosolar.bigdata.flink.connectors.jdbc.connection.JdbcConnectionProvider;
import com.aikosolar.bigdata.flink.connectors.jdbc.connection.SimpleJdbcConnectionProvider;
import com.aikosolar.bigdata.flink.connectors.jdbc.writter.JdbcWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * @author carlc
 */
public class JdbcMergeSink<T> extends RichSinkFunction<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMergeSink.class);

    private final String checkSql;
    private final String updateSql;
    private final String insertSql;
    private final JdbcConnectionProvider connectionProvider;
    private final JdbcConnectionOptions connectionOptions;

    private final JdbcWriter<T> writer;
    private transient Connection connection;
    private transient PreparedStatement stmt;
    private transient PreparedStatement checkStmt;
    private transient PreparedStatement updateStmt;

    public JdbcMergeSink(JdbcConnectionOptions connectionOptions, String checkSql, String updateSql, String insertSql, JdbcWriter writer) {
        this.checkSql = checkSql;
        this.updateSql = updateSql;
        this.insertSql = insertSql;
        this.writer = writer;
        this.connectionOptions = connectionOptions;
        this.connectionProvider = new SimpleJdbcConnectionProvider(connectionOptions);

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = connectionProvider.getConnection();
        stmt = connection.prepareStatement(insertSql);
        checkStmt = connection.prepareStatement(checkSql);
        updateStmt = connection.prepareStatement(updateSql);
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        for (int i = 1; i <= connectionOptions.getMaxRetries(); i++) {
            try {
                if (this.writer.exsits(checkStmt, value)) {
                    this.writer.update(updateStmt, value);
                    updateStmt.executeUpdate();
                } else {
                    this.writer.accept(stmt, value);
                    stmt.executeUpdate();
                }

                break;
            } catch (SQLException e) {
                if (i >= connectionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    // 如果连接异常,则尝试重新获取连接,并准备Statement
                    if (!connection.isValid(connectionOptions.getCheckTimeoutSeconds())) {
                        this.connection = connectionProvider.reestablishConnection();
                        IOUtils.closeQuietly(stmt);
                        IOUtils.closeQuietly(updateStmt);
                        IOUtils.closeQuietly(checkStmt);
                        this.stmt = connection.prepareStatement(this.insertSql);
                        this.updateStmt = connection.prepareStatement(this.updateSql);
                        this.checkStmt = connection.prepareStatement(this.checkSql);
                    }
                } catch (Exception ex) {
                    LOGGER.error("JDBC connection is not valid, and reestablish connection failed.", ex);
                    throw new IOException("Reestablish JDBC connection failed", ex);
                }
                try {
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Unable to write data; interrupted while doing another attempt", e);
                }
            } catch (Exception e) {
                if (e instanceof ParseException) {
                    //  no-op 日期格式不正确,我们不处理
                } else {
                    throw new IOException(e.getMessage(), e);
                }
            }
        }
    }


    @Override
    public void close() {
        if (stmt != null) {
            try {
                this.writer.flush();
            } catch (Exception e) {
                // no-op
            }
            IOUtils.closeQuietly(stmt);
        }
        IOUtils.closeQuietly(updateStmt);
        IOUtils.closeQuietly(checkStmt);
        IOUtils.closeQuietly(connection);
    }
}
