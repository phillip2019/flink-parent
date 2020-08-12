package com.aikosolar.debezium.study;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * @author carlc
 */
public class DebeziumTest {

    //  https://debezium.io/documentation/reference/1.2/development/engine.html
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("name", "engine");
        properties.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        properties.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        properties.setProperty("offset.flush.interval.ms", "60000");
        properties.setProperty("database.hostname", "localhost");
        properties.setProperty("database.port", "3306");
        properties.setProperty("database.user", "root");
        properties.setProperty("database.password", "123456");
//        properties.setProperty("database.server.id", "85744"); // 先不用
        properties.setProperty("database.server.name", "mysql-binlog-connector");
        properties.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        properties.setProperty("database.history.file.filename", "dbhistory.dat");


//        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = DebeziumEngine.create(Connect.class)
//                .using(properties)
//                .notifying((records, committer) -> {
//                    for (ChangeEvent<SourceRecord, SourceRecord> r : records) {
//                        System.out.println(r);
//                        committer.markProcessed(r);
//                    }
//                })
//                .using((success, message, error) -> {
//                    // 所有参数都看不懂,先打印吧
//                    System.out.println(success);
//                    System.out.println(message);
//                    System.out.println(error);
//                })
//                .build();

        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(properties)
                // 消息通知
                .notifying((records, committer) -> {
                    for (ChangeEvent<String, String> r : records) {
                        System.out.println("Key = '" + r.key() + "' value = '" + r.value() + "'");
                        committer.markProcessed(r);
                    }
                })
                .using((success, message, error) -> {
                    // 所有参数都看不懂,先打印吧
                    System.out.println(success);
                    System.out.println(message);
                    System.out.println(error);
                })
                .build();

        Executors.newSingleThreadExecutor().execute(engine);

    }
}
