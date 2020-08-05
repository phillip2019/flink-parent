package com.aikosolar.bigdata.flink.connectors.jdbc.dialect;

import java.io.Serializable;
import java.util.Optional;

/**
 * 方言
 *
 * @author carlc
 */
public interface JdbcDialect extends Serializable {

    /**
     * 方言名称
     */
    String dialectName();

    /**
     * url是否能被当前方言处理
     */
    boolean canHandle(String url);

    /**
     * 获取Upsert语句,如果返回null,则使用select + update/insert处理
     */
    default Optional<String> getUpsertStatement() {
        return Optional.empty();
    }

    /**
     * 获取Update语句
     */
    Optional<String> getUpdateStatement();

    /**
     * 获取Insert语句
     */
    Optional<String> getInsertStatement();

    /**
     * 获取判断行是否存在语句
     */
    Optional<String> getRowExistsStatement();

    /**
     * 默认DriverName
     */
    default Optional<String> defaultDriverName() {
        return Optional.empty();
    }

}
