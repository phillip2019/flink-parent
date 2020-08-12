package com.aikosolar.debezium.study;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
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
        properties.setProperty("database.hostname", "mysql");
        properties.setProperty("database.port", "3306");
        properties.setProperty("database.user", "root");
        properties.setProperty("database.password", "root");
//        properties.setProperty("database.server.id", "85744"); // 先不用
        properties.setProperty("database.server.name", "mysql-binlog-connector");
        properties.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        properties.setProperty("database.history.file.filename", "/path/to/storage/dbhistory.dat");


        DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = DebeziumEngine.create(Connect.class)
                .using(properties)
                .notifying(new DebeziumEngine.ChangeConsumer<ChangeEvent<SourceRecord, SourceRecord>>() {
                    @Override
                    public void handleBatch(List<ChangeEvent<SourceRecord, SourceRecord>> records, DebeziumEngine.RecordCommitter<ChangeEvent<SourceRecord, SourceRecord>> committer) throws InterruptedException {
                        // 所有参数都看不懂,先打印吧
                        System.out.println(records);
                        System.out.println(committer);
                    }
                })
                .using(new DebeziumEngine.CompletionCallback() {
                    @Override
                    public void handle(boolean success, String message, Throwable error) {
                        // 所有参数都看不懂,先打印吧
                        System.out.println(success);
                        System.out.println(message);
                        System.out.println(error);
                    }
                })
                .build();

        Executors.newSingleThreadExecutor().execute(engine);

    }
}
