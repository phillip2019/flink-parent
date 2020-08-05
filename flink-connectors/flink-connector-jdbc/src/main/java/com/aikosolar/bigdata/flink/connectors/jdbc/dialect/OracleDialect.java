package com.aikosolar.bigdata.flink.connectors.jdbc.dialect;

import java.util.Optional;

/**
 * @author carlc
 */
public class OracleDialect extends AbstractDialect {

    @Override
    public String dialectName() {
        return "Oracle";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle:");
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
        return Optional.of("oracle.jdbc.OracleDriver");
    }
}
