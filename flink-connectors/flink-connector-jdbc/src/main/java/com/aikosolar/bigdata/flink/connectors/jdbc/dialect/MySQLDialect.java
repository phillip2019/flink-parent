package com.aikosolar.bigdata.flink.connectors.jdbc.dialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author carlc
 */
public class MySQLDialect extends AbstractDialect {

    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public Optional<String> getUpsertStatement() {
        String insert = getInsertStatement().get();
        List<String> fields = new ArrayList<>();  // todo 提取字段
        String subSql = fields.stream().map(x -> x + "=VALUES(" + x + ")").collect(Collectors.joining(", "));
//      return Optional.of(insert + " ON DUPLICATE KEY UPDATE " + subSql);
        throw new UnsupportedOperationException("not impl yet");
    }


    @Override
    public Optional<String> getUpdateStatement() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getInsertStatement() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getRowExistsStatement() {
        return Optional.empty();
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }
}
